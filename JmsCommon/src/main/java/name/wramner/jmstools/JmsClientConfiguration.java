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

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;

import name.wramner.jmstools.counter.AtomicCounter;
import name.wramner.jmstools.counter.Counter;

import org.kohsuke.args4j.Option;

/**
 * Base class for JMS client configuration classes. Includes options common to both consumers and producers.
 * 
 * @author Erik Wramner
 */
public abstract class JmsClientConfiguration {
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
