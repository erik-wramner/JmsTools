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

import java.security.NoSuchAlgorithmException;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;

import org.kohsuke.args4j.Option;

import com.rabbitmq.jms.admin.RMQConnectionFactory;

import name.wramner.jmstools.consumer.RabbitJmsConsumer.RabbitConsumerConfiguration;

/**
 * Command line JMS Rabbit MQ message consumer intended for benchmarks and other tests.
 * 
 * @author Erik Wramner
 */
public class RabbitJmsConsumer extends JmsConsumer<RabbitConsumerConfiguration> {

    /**
     * Program entry point.
     * 
     * @param args Command line.
     * @see RabbitConsumerConfiguration
     */
    public static void main(String[] args) {
        new RabbitJmsConsumer().run(args);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RabbitConsumerConfiguration createConfiguration() {
        return new RabbitConsumerConfiguration();
    }

    /**
     * Rabbit MQ consumer configuration. It extends the basic JMS consumer configuration with Rabbit MQ-specific
     * settings.
     * 
     * @author Erik Wramner
     */
    public static class RabbitConsumerConfiguration extends JmsConsumerConfiguration {
        @Option(name = "-uri", aliases = { "--jms-uri" }, usage = "AMQP URI for RabbitMQ connection")
        private String _uri;

        @Option(name = "-user", aliases = { "--jms-user" }, usage = "User name if using authentication")
        private String _userName;

        @Option(name = "-pw", aliases = { "--jms-password" }, usage = "Password if using authentication", depends = {
                        "-user" })
        private String _password;

        @Option(name = "-vhost", aliases = { "--jms-virtual-host" }, usage = "Virtual host for Rabbit MQ")
        private String _virtualHost = "/";

        @Option(name = "-host", aliases = { "--jms-host" }, usage = "Host or IP address for Rabbit MQ")
        private String _host;

        @Option(name = "-ssl", aliases = { "--jms-use-ssl" }, usage = "Enable/disable SSL for Rabbit MQ connection")
        private boolean _ssl;

        @Option(name = "-port", aliases = { "--jms-port" }, usage = "Port for Rabbit MQ")
        private Integer _port;

        public ConnectionFactory createConnectionFactory() throws JMSException {
            RMQConnectionFactory cf = new RMQConnectionFactory();
            if (_host != null) {
                cf.setHost(_host);
                cf.setPort(_port != null ? _port.intValue() : (_ssl ? 5671 : 5672));
                cf.setVirtualHost(_virtualHost);
                cf.setUsername(_userName);
                cf.setPassword(_password);
                if (_ssl) {
                    try {
                        cf.useSslProtocol();
                    } catch (NoSuchAlgorithmException e) {
                        throw new JMSException("SSL not supported!");
                    }
                }
            }
            if (_uri != null) {
                cf.setUri(_uri);
            }
            return cf;
        }

        public XAConnectionFactory createXAConnectionFactory() throws JMSException {
            throw new JMSException("RabbitMQ does not support JMS XA connections");
        }
    }
}
