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
package name.wramner.jmstools.consumer;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.kohsuke.args4j.Option;

import name.wramner.jmstools.consumer.ArtemisJmsConsumer.ArtemisConsumerConfiguration;

/**
 * Command line JMS Artemis message consumer intended for benchmarks and other tests.
 *
 * @author Erik Wramner
 * @author Anton Roskvist
 */
public class ArtemisJmsConsumer extends JmsConsumer<ArtemisConsumerConfiguration> {

    /**
     * Program entry point.
     *
     * @param args Command line.
     * @see ArtemisConsumerConfiguration
     */
    public static void main(String[] args) {
        new ArtemisJmsConsumer().run(args);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ArtemisConsumerConfiguration createConfiguration() {
        return new ArtemisConsumerConfiguration();
    }

    /**
     * Artemis consumer configuration. It extends the basic JMS consumer configuration with Artemis-specific settings
     * such as broker URL, user and password needed in order to connect to the Artemis broker.
     *
     * @author Erik Wramner
     * @author Anton Roskvist
     */
    public static class ArtemisConsumerConfiguration extends JmsConsumerConfiguration {
        @Option(name = "-url", aliases = { "--jms-broker-url" }, usage = "Artemis broker URL", required = true)
        private String _brokerUrl;

        @Option(name = "-user", aliases = { "--jms-user",
                        "--jms-broker-user" }, usage = "Artemis user name if using authentication")
        private String _userName;

        @Option(name = "-pw", aliases = { "--jms-password",
                        "--jms-broker-password" }, usage = "Artemis password if using authentication", depends = {
                                        "-user" })
        private String _password;

        @Override
        public ConnectionFactory createConnectionFactory() throws JMSException {
            if (_userName != null && _password != null) {
                return new ActiveMQConnectionFactory(_brokerUrl, _userName, _password);
            } else {
                return new ActiveMQConnectionFactory(_brokerUrl);
            }
        }

        @Override
        public XAConnectionFactory createXAConnectionFactory() throws JMSException {
            if (_userName != null && _password != null) {
                return new ActiveMQXAConnectionFactory(_brokerUrl, _userName, _password);
            } else {
                return new ActiveMQXAConnectionFactory(_brokerUrl);
            }
        }
    }
}
