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

import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.kohsuke.args4j.Option;

import name.wramner.jmstools.producer.AmqJmsProducer.AmqProducerConfiguration;

/**
 * Command line JMS ActiveMQ message producer intended for benchmarks and other tests.
 * 
 * @author Erik Wramner
 */
public class AmqJmsProducer extends JmsProducer<AmqProducerConfiguration> {

    /**
     * Program entry point.
     * 
     * @param args Command line.
     * @see AmqProducerConfiguration
     */
    public static void main(String[] args) {
        new AmqJmsProducer().run(args);
    }

    /**
     * Create configuration specific to the AQ client.
     * 
     * @return configuration instance.
     * @see name.wramner.jmstools.producer.JmsProducer#createConfiguration()
     */
    @Override
    protected AmqProducerConfiguration createConfiguration() {
        return new AmqProducerConfiguration();
    }

    /**
     * ActiveMQ producer configuration. It extends the basic JMS producer configuration with ActiveMQ-specific settings
     * such as broker URL, user and password needed in order to connect to the ActiveMQ broker.
     * 
     * @author Erik Wramner
     */
    public static class AmqProducerConfiguration extends JmsProducerConfiguration {
        @Option(name = "-url", aliases = { "--jms-broker-url" }, usage = "ActiveMQ broker URL", required = true)
        private String _brokerUrl;

        @Option(name = "-user", aliases = { "--jms-user" }, usage = "ActiveMQ user name if using authentication")
        private String _userName;

        @Option(name = "-pw", aliases = { "--jms-broker-password" }, usage = "ActiveMQ password if using authentication", depends = { "-user" })
        private String _password;

        @Override
        public DelayedDeliveryAdapter createDelayedDeliveryAdapter() {
            return new DelayedDeliveryAdapter() {

                @Override
                public void setDelayProperty(Message msg, int seconds) throws JMSException {
                    msg.setLongProperty("AMQ_SCHEDULED_DELAY", TimeUnit.MILLISECONDS.convert(seconds, TimeUnit.SECONDS));
                }
            };
        }

        public ConnectionFactory createConnectionFactory() throws JMSException {
            if (_userName != null && _password != null) {
                return new ActiveMQConnectionFactory(_userName, _password, _brokerUrl);
            } else {
                return new ActiveMQConnectionFactory(_brokerUrl);
            }
        }
    }
}
