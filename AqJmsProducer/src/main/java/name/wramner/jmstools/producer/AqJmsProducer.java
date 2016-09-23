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
package name.wramner.jmstools.producer;

import javax.jms.*;

import org.kohsuke.args4j.Option;

import name.wramner.jmstools.aq.AqJmsUtils;
import name.wramner.jmstools.aq.AqObjectMessageAdapter;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.ObjectMessageAdapter;
import name.wramner.jmstools.producer.AqJmsProducer.AqProducerConfiguration;

/**
 * Command line JMS AQ message producer intended for benchmarks and other tests.
 *
 * @author Erik Wramner
 */
public class AqJmsProducer extends JmsProducer<AqProducerConfiguration> {

    /**
     * Program entry point.
     *
     * @param args Command line.
     * @see AqProducerConfiguration
     */
    public static void main(String[] args) {
        new AqJmsProducer().run(args);
    }

    /**
     * Create configuration specific to the AQ client.
     *
     * @return configuration instance.
     * @see name.wramner.jmstools.producer.JmsProducer#createConfiguration()
     */
    @Override
    protected AqProducerConfiguration createConfiguration() {
        return new AqProducerConfiguration();
    }

    /**
     * AQ producer configuration. It extends the basic JMS producer configuration with AQ-specific settings. Primarily
     * that means the JDBC URL, user and password needed in order to connect to the AQ database. In addition it is
     * possible to configure a flow controller that polls the queue depth in the database and pauses/resumes the
     * producer threads in order to avoid overloading the consumers. For long-running tests where it is difficult to
     * predict how many consumer and producer threads that are needed this can be very useful.
     *
     * @author Erik Wramner
     */
    public static class AqProducerConfiguration extends JmsProducerConfiguration {
        @Option(name = "-url", aliases = { "--aq-jdbc-url" }, usage = "JDBC URL for AQ database connection", required = true)
        private String _aqJdbcUrl;

        @Option(name = "-user", aliases = { "--aq-jdbc-user" }, usage = "JDBC user for AQ database connection", required = true)
        private String _aqJdbcUser;

        @Option(name = "-pw", aliases = { "--aq-jdbc-password" }, usage = "JDBC password for AQ database connection", required = true)
        private String _aqJdbcPassword;

        @Option(name = "-pause", aliases = { "--flow-control-pause-at" }, usage = "Pause load at given backlog (approximate)")
        private Integer _flowControlPauseAtBacklog;

        @Option(name = "-resume", aliases = { "--flow-control-resume-at" }, usage = "Resume load at given backlog (approximate)", depends = { "-pause" })
        private int _flowControlResumeAtBacklog = 0;

        @Option(name = "-flowint", aliases = { "--flow-control-check-interval-seconds" }, usage = "Time between flow control checks")
        private int _flowControlCheckIntervalSeconds = 20;

        @Override
        public Counter createMessageCounter() {
            Counter counter = super.createMessageCounter();
            if (_flowControlPauseAtBacklog != null) {
                return new FlowControllingCounter(counter, new AqFlowController(_aqJdbcUrl, _aqJdbcUser,
                                _aqJdbcPassword, _flowControlPauseAtBacklog.intValue(), _flowControlResumeAtBacklog,
                                getQueueName(), _flowControlCheckIntervalSeconds));
            } else {
                return counter;
            }
        }

        @Override
        public DelayedDeliveryAdapter createDelayedDeliveryAdapter() {
            return new DelayedDeliveryAdapter() {

                @Override
                public void setDelayProperty(Message msg, int seconds) throws JMSException {
                    msg.setIntProperty("JMS_OracleDelay", seconds);
                }
            };
        }

        @Override
        public ConnectionFactory createConnectionFactory() throws JMSException {
            return AqJmsUtils.createConnectionFactory(_aqJdbcUrl, _aqJdbcUser, _aqJdbcPassword);
        }

        @Override
        public XAConnectionFactory createXAConnectionFactory() throws JMSException {
            return AqJmsUtils.createXAConnectionFactory(_aqJdbcUrl, _aqJdbcUser, _aqJdbcPassword);
        }

        @Override
        public ObjectMessageAdapter getObjectMessageAdapter() {
            return new AqObjectMessageAdapter();
        }
    }
}
