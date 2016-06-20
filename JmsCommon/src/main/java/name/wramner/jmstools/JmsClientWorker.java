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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.rm.ResourceManagerFactory;
import name.wramner.jmstools.stopcontroller.StopController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for consumer and producer workers.
 * 
 * @author Erik Wramner
 * @param <T> The configuration class.
 */
public abstract class JmsClientWorker<T extends JmsClientConfiguration> implements Runnable {
    private static final long JMS_EXCEPTION_RECOVERY_TIME_MS = 10000L;
    protected static final String JMS_PROPNAME_UNIQUE_MESSAGE_ID = "Unique-Message-Id";
    protected final Logger _logger = LoggerFactory.getLogger(getClass());
    protected final Random _random = new Random();
    protected final ResourceManagerFactory _resourceManagerFactory;
    protected final StopController _stopController;
    protected final Counter _messageCounter;
    private final File _logFile;
    private final boolean _rollbacksEnabled;
    private final double _rollbackProbability;

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
        if (_rollbacksEnabled = config.getRollbackPercentage() != null) {
            _rollbackProbability = config.getRollbackPercentage().doubleValue() / 100.0;
        } else {
            _rollbackProbability = 0.0;
        }
    }

    /**
     * Create output stream for message logging if enabled, then process messages as long as the stop controller wants
     * to keep running. If an exception occurs, sleep for a while before trying again. On unexpected exceptions, bail
     * out.
     */
    @Override
    public void run() {
        _logger.debug("Worker starting...");
        OutputStream os = null;
        try {
            if (_logFile != null) {
                os = new BufferedOutputStream(new FileOutputStream(_logFile));
            }
            while (_stopController.keepRunning()) {
                processMessages(os);
                recoverAfterException();
            }
        } catch (Exception e) {
            _logger.error("Worker failed, aborting!", e);
        } finally {
            if (os != null) {
                flushSafely(os);
                closeSafely(os);
            }
            _logger.debug("Worker stopped");
        }
    }

    /**
     * Process messages and log to output stream unless it is null. Return when done or on exceptions.
     * 
     * @param os The output stream.
     * @throws IOException on I/O errors.
     */
    protected abstract void processMessages(OutputStream os) throws IOException;

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
    protected boolean shouldRollback() {
        return _rollbacksEnabled && _random.nextDouble() < _rollbackProbability;
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
