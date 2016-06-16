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

import name.wramner.jmstools.consumer.AmqJmsConsumer.AmqConsumerConfiguration;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.kohsuke.args4j.Option;

/**
 * Command line JMS ActiveMQ message consumer intended for benchmarks and other tests.
 * 
 * @author Erik Wramner
 */
public class AmqJmsConsumer extends JmsConsumer<AmqConsumerConfiguration> {

    /**
     * Program entry point.
     * 
     * @param args Command line.
     * @see AmqConsumerConfiguration
     */
    public static void main(String[] args) {
        new AmqJmsConsumer().run(args);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AmqConsumerConfiguration createConfiguration() {
        return new AmqConsumerConfiguration();
    }

    /**
     * ActiveMQ consumer configuration. It extends the basic JMS consumer configuration with ActiveMQ-specific settings
     * such as broker URL, user and password needed in order to connect to the ActiveMQ broker.
     * 
     * @author Erik Wramner
     */
    public static class AmqConsumerConfiguration extends JmsConsumerConfiguration {
        @Option(name = "-url", aliases = { "--jms-broker-url" }, usage = "ActiveMQ broker URL", required = true)
        private String _brokerUrl;

        @Option(name = "-user", aliases = { "--jms-user" }, usage = "ActiveMQ user name if using authentication")
        private String _userName;

        @Option(name = "-pw", aliases = { "--jms-broker-password" }, usage = "ActiveMQ password if using authentication", depends = { "-user" })
        private String _password;

        public ConnectionFactory createConnectionFactory() throws JMSException {
            if (_userName != null && _password != null) {
                return new ActiveMQConnectionFactory(_userName, _password, _brokerUrl);
            } else {
                return new ActiveMQConnectionFactory(_brokerUrl);
            }
        }

        public XAConnectionFactory createXAConnectionFactory() throws JMSException {
            if (_userName != null && _password != null) {
                return new ActiveMQXAConnectionFactory(_userName, _password, _brokerUrl);
            } else {
                return new ActiveMQXAConnectionFactory(_brokerUrl);
            }
        }
    }
}
