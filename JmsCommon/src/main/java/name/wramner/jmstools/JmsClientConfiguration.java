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

import java.io.File;

import javax.jms.*;

import name.wramner.jmstools.counter.AtomicCounter;
import name.wramner.jmstools.counter.Counter;

import org.kohsuke.args4j.Option;

/**
 * Base class for JMS client configuration classes. Includes options common to both consumers and producers.
 * 
 * @author Erik Wramner
 */
public abstract class JmsClientConfiguration {
    private static final int DEFAULT_JTA_TIMEOUT_SECONDS = 300;
    private static final int DEFAULT_TM_CHECKPOINT_INTERVAL_SECONDS = 30;
    private static final int DEFAULT_TM_RECOVERY_INTERVAL_SECONDS = 60;

    @Option(name = "-t", aliases = { "--threads" }, usage = "Number of threads")
    protected int _threads = 1;

    @Option(name = "-queue", aliases = { "--queue-name" }, usage = "Queue name")
    protected String _queueName = "test_queue";

    @Option(name = "-count", aliases = { "--stop-after-messages" }, usage = "Total number of messages to process")
    protected Integer _stopAfterMessages;

    @Option(name = "-duration", aliases = { "--duration-minutes" }, usage = "Duration to run in minutes")
    protected Integer _durationMinutes;

    @Option(name = "-stats", aliases = "--log-statistics", usage = "Log statistics every minute")
    protected boolean _stats = true;

    @Option(name = "-rollback", aliases = "--rollback-percentage", usage = "Percentage to rollback rather than commit, decimals supported")
    protected Double _rollbackPercentage;

    @Option(name = "-log", aliases = "--log-directory", usage = "Directory for detailed message logs, enables message logging")
    protected File _logDirectory;

    @Option(name = "-xa", aliases = "--xa-transactions", usage = "Use XA (two-phase) transactions")
    protected boolean _useXa;

    @Option(name = "-tmname", aliases = "--xa-tm-name", usage = "XA: The unique transaction manager name", depends = { "-xa" })
    protected String _tmName;

    @Option(name = "-tmlogs", aliases = "--xa-tm-log-directory", usage = "XA: The path to the transaction manager logs", depends = { "-xa" }, forbids = { "-notmlog" })
    protected File _xaLogBaseDir;

    @Option(name = "-jtatimeout", aliases = "--xa-jta-timeout-seconds", usage = "XA: The transaction timeout", depends = { "-xa" })
    protected int _jtaTimeoutSeconds = DEFAULT_JTA_TIMEOUT_SECONDS;

    @Option(name = "-notmlog", aliases = "--xa-no-tm-log", usage = "XA: Disable transaction logs for raw performance", depends = { "-xa" }, forbids = { "-tmlogs" })
    private boolean _noTmLog;

    @Option(name = "-tmrecint", aliases = "--xa-tm-recovery-interval-seconds", usage = "XA: Time in seconds between two recovery scans", depends = { "-xa" })
    private int _recoveryIntervalSeconds = DEFAULT_TM_RECOVERY_INTERVAL_SECONDS;

    @Option(name = "-tmchkint", aliases = "--xa-tm-checkpoint-interval-seconds", usage = "XA: Time in seconds between two checkpoints for the transaction log", depends = { "-xa" })
    private long _checkpointIntervalSeconds = DEFAULT_TM_CHECKPOINT_INTERVAL_SECONDS;

    /**
     * Get the number of threads to use.
     *
     * @return number of threads.
     */
    public int getThreads() {
        return _threads;
    }

    /**
     * Get the queue name.
     *
     * @return queue name.
     */
    public String getQueueName() {
        return _queueName;
    }

    /**
     * Check if statistics should be logged every minute. Statistics are cheap.
     *
     * @return true to log statistics.
     */
    public boolean isStatisticsEnabled() {
        return _stats;
    }

    /**
     * Get the percentage of transactions (message batches) to roll back.
     *
     * @return rollback percentage or null for none.
     */
    public Double getRollbackPercentage() {
        return _rollbackPercentage;
    }

    /**
     * Get the directory for message logs. It is used if every produced/consumed message is logged with unique
     * identities, making it possible to verify that no messages have been lost or delivered multiple times.
     *
     * @return log directory.
     */
    public File getLogDirectory() {
        return _logDirectory;
    }

    /**
     * Create a thread-safe counter for received messages.
     *
     * @return message counter.
     */
    public Counter createMessageCounter() {
        return new AtomicCounter();
    }

    /**
     * Check if XA transactions are enabled.
     *
     * @return true for XA, false for standard.
     */
    public boolean useXa() {
        return _useXa;
    }

    /**
     * Check if XA transaction manager logs have been disabled. Without the logs there is little security in the XA
     * protocol, but then this is a test tool. It will give a false impression of the actual real-world performance,
     * though.
     *
     * @return true if the tm log should be disabled.
     */
    public boolean isTmLogDisabled() {
        return _noTmLog;
    }

    /**
     * Get the unique transaction manager name for XA transactions. This is optional, but if the same client is started
     * multiple times on the same machine the default name will not be unique. In that case the option must be used.
     *
     * @return transaction manager name (must be unique).
     */
    public String getTmName() {
        return _tmName;
    }

    /**
     * Get the base directory for XA transaction manager logs. As this is a test tool the logs are probably not
     * critical, but for real systems they are vital. Pick a fast and reliable disk if possible.
     *
     * @return transaction manager log directory.
     */
    public File getXaLogBaseDir() {
        return _xaLogBaseDir;
    }

    /**
     * Get the XA transaction timeout in seconds.
     *
     * @return timeout for global transactions in seconds.
     */
    public int getJtaTimeoutSeconds() {
        return _jtaTimeoutSeconds;
    }

    /**
     * Get the time between two recovery scans in seconds.
     *
     * @return recovery interval.
     */
    public int getRecoveryIntervalSeconds() {
        return _recoveryIntervalSeconds;
    }

    /**
     * Get the time between two checkpoints for the transaction log in seconds. A checkpoint reduces the size of the
     * transaction log, but imposes some overhead.
     * 
     * @return checkpoint interval.
     */
    public long getCheckpointIntervalSeconds() {
        return _checkpointIntervalSeconds;
    }

    /**
     * Create a JMS connection factory for normal transactions.
     *
     * @return connection factory.
     * @throws JMSException on errors.
     */
    public abstract ConnectionFactory createConnectionFactory() throws JMSException;

    /**
     * Create a JMS connection factory for XA transactions.
     *
     * @return connection factory.
     * @throws JMSException on errors.
     */
    public abstract XAConnectionFactory createXAConnectionFactory() throws JMSException;
}
