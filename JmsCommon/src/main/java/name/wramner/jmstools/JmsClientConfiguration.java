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

import name.wramner.jmstools.counter.AtomicCounter;
import name.wramner.jmstools.counter.Counter;

import org.kohsuke.args4j.Option;

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

    public int getThreads() {
        return _threads;
    }

    public String getQueueName() {
        return _queueName;
    }

    public boolean isStatisticsEnabled() {
        return _stats;
    }

    public Double getRollbackPercentage() {
        return _rollbackPercentage;
    }

    public File getLogDirectory() {
        return _logDirectory;
    }

    public Counter createMessageCounter() {
        return new AtomicCounter();
    }

    public abstract ConnectionFactory createConnectionFactory() throws JMSException;
}
