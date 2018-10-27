/*
 * Copyright 2016 Erik Wramner.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package name.wramner.jmstools;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.jms.JMSException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.ObjectMessageAdapter;
import name.wramner.jmstools.rm.ResourceManager;
import name.wramner.jmstools.rm.ResourceManagerFactory;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * Base class for consumer and producer workers.
 *
 * @author Erik Wramner
 * @param <T> The configuration class.
 */
public abstract class JmsClientWorker<T extends JmsClientConfiguration> implements Runnable {
    private static final long JMS_EXCEPTION_RECOVERY_TIME_MS = 10000L;
    protected final Logger _logger = LoggerFactory.getLogger(getClass());
    protected final Random _random = new Random();
    protected final ResourceManagerFactory _resourceManagerFactory;
    protected final StopController _stopController;
    protected final Counter _messageCounter;
    protected final ObjectMessageAdapter _objectMessageAdapter;
    private final File _logFile;
    private final boolean _rollbacksEnabled;
    private final double _rollbackProbability;
    private final List<String[]> _pendingLogEntries;
    private final boolean _abortOnError;
    private final long _commitDelayMillis;
    private OutputStream _os;

    /**
     * Constructor.
     *
     * @param resourceManagerFactory The resource manager factory.
     * @param counter The counter for processed messages.
     * @param stopController The stop controller.
     * @param logFile The log file for messages or null.
     * @param config The configuration for other options.
     */
    protected JmsClientWorker(ResourceManagerFactory resourceManagerFactory, Counter counter,
                    StopController stopController, File logFile, T config) {
        _resourceManagerFactory = resourceManagerFactory;
        _stopController = stopController;
        _messageCounter = counter;
        _logFile = logFile;
        _rollbacksEnabled = config.getRollbackPercentage() != null;
        if (_rollbacksEnabled) {
            _rollbackProbability = config.getRollbackPercentage().doubleValue() / 100.0;
        } else {
            _rollbackProbability = 0.0;
        }
        _pendingLogEntries = new ArrayList<String[]>();
        _objectMessageAdapter = config.getObjectMessageAdapter();
        _abortOnError = config.isAbortOnErrorEnabled();
        _commitDelayMillis = config.getCommitDelayMillis() != null ? config.getCommitDelayMillis().longValue() : 0L;
    }

    /**
     * Create output stream for message logging if enabled, then process messages as long as the stop controller wants
     * to keep running. If an exception occurs, sleep for a while before trying again. On unexpected exceptions, bail
     * out. If the abort on error option has been selected, bail out on all errors.
     */
    @Override
    public void run() {
        _logger.debug("Worker starting...");
        try {
            initMessageLogIfEnabled();
            while (_stopController.keepRunning()) {
                if (!processMessages()) {
                    if (_abortOnError) {
                        _logger.error("Worker failed and abort on error configured, stopping!");
                        _stopController.abort();
                    } else {
                        recoverAfterException();
                    }
                }
            }
        } catch (Exception e) {
            _logger.error("Worker failed, aborting!", e);
        } finally {
            cleanupMessageLog();
            _logger.debug("Worker stopped");
        }
    }

    /**
     * Check if messages should be logged.
     *
     * @return true if logging is enabled.
     */
    protected boolean messageLogEnabled() {
        return _logFile != null;
    }

    /**
     * Process messages until done (success) or until an error occurs.
     *
     * @return true on success.
     */
    private boolean processMessages() {
        try (ResourceManager resourceManager = _resourceManagerFactory.createResourceManager()) {
            processMessages(resourceManager);
            return true;
        } catch (JMSException e) {
            _logger.error("JMS error!", e);
            logPendingMessagesRolledBack();
        } catch (RollbackException | HeuristicRollbackException e) {
            _logger.error("Failed to commit!", e);
            logPendingMessagesRolledBack();
        } catch (HeuristicMixedException e) {
            _logger.error("Failed to commit, but part of the transaction MAY have completed!", e);
            logPendingMessagesInDoubt();
        }
        return false;
    }

    /**
     * Process messages until the stop controller is satisfied or until an error occurs.
     *
     * @param resourceManager The resource manager for transaction control.
     * @throws JMSException on JMS errors.
     * @throws RollbackException when the XA resource has been rolled back.
     * @throws HeuristicRollbackException when the XA resource has been rolled back heuristically.
     * @throws HeuristicMixedException when the XA resource has been rolled back OR committed.
     */
    protected abstract void processMessages(ResourceManager resourceManager)
                    throws RollbackException, JMSException, HeuristicMixedException, HeuristicRollbackException;

    /**
     * Get header names for the detailed message log.
     *
     * @return header names.
     */
    protected abstract String[] getMessageLogHeaders();

    /**
     * Commit or roll back depending on the configured roll back probability.
     *
     * @param resourceManager the resource manager.
     * @param messageCount The number of messages to commit/roll back.
     * @throws JMSException on JMS errors.
     * @throws RollbackException if a commit fails as the resource has been rolled back.
     * @throws HeuristicRollbackException if a commit fails as the resource has been rolled back heuristically.
     * @throws HeuristicMixedException if a commit fails as the resource has been rolled back OR committed
     *         heuristically.
     */
    protected void commitOrRollback(ResourceManager resourceManager, int messageCount)
                    throws JMSException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
        if (_commitDelayMillis > 0L) {
            _stopController.waitForTimeoutOrDone(_commitDelayMillis);
        }
        if (shouldRollback()) {
            resourceManager.rollback();
            logPendingMessagesRolledBack();
        } else {
            resourceManager.commit();
            _messageCounter.incrementCount(messageCount);
            logPendingMessagesCommitted();
        }
    }

    /**
     * Add fields for logging a consumed or produced message.
     *
     * @param fields The fields to log.
     */
    protected void logMessage(String... fields) {
        if (_os != null) {
            _pendingLogEntries.add(fields);
        }
    }

    /**
     * Wait for a while in the face of an exception. The typical scenario is that a JMS exception has occurred, perhaps
     * because the JMS server has failed. A standby server may come up in short order, but hammering it is not likely to
     * help.
     */
    private void recoverAfterException() {
        _stopController.waitForTimeoutOrDone(JMS_EXCEPTION_RECOVERY_TIME_MS);
    }

    /**
     * Check if this transaction should be rolled back.
     *
     * @return true to roll back.
     */
    private boolean shouldRollback() {
        return _rollbacksEnabled && _random.nextDouble() < _rollbackProbability;
    }

    private void cleanupMessageLog() {
        if (_os != null) {
            flushSafely(_os);
            closeSafely(_os);
        }
    }

    private void initMessageLogIfEnabled() throws FileNotFoundException {
        if (messageLogEnabled()) {
            _os = new BufferedOutputStream(new FileOutputStream(_logFile));
            logHeader();
        }
    }

    private void logPendingMessagesCommitted() {
        logPendingMessages("C");
    }

    private void logPendingMessagesRolledBack() {
        logPendingMessages("R");
    }

    private void logPendingMessagesInDoubt() {
        logPendingMessages("?");
    }

    private void logHeader() {
        if (_os != null) {
            StringBuilder sb = new StringBuilder(256);
            sb.append("State");
            sb.append('\t');
            sb.append("CommitTime");
            for (String header : getMessageLogHeaders()) {
                sb.append('\t');
                sb.append(header);
            }
            sb.append('\n');
            write(sb);
        }
    }

    private void logPendingMessages(String action) {
        if (_os != null) {
            StringBuilder sb = new StringBuilder(256);
            String nowString = String.valueOf(System.currentTimeMillis());
            for (String[] fields : _pendingLogEntries) {
                sb.append(action);
                sb.append('\t');
                sb.append(nowString);
                for (String field : fields) {
                    sb.append('\t');
                    if(field != null) {
                        sb.append(field);
                    }
                }
                sb.append('\n');
            }
            write(sb);
            _pendingLogEntries.clear();
        }
    }

    private void write(StringBuilder sb) {
        try {
            _os.write(sb.toString().getBytes(Charset.defaultCharset()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void closeSafely(OutputStream os) {
        try {
            os.close();
        } catch (IOException e) {
        }
    }

    private static void flushSafely(OutputStream os) {
        try {
            os.flush();
        } catch (IOException e) {
        }
    }
}
