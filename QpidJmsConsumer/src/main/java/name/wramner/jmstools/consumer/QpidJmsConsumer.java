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

import java.util.Hashtable;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.kohsuke.args4j.Option;

import name.wramner.jmstools.consumer.QpidJmsConsumer.QpidConsumerConfiguration;

/**
 * Command line JMS Apache Qpid message consumer intended for benchmarks and other tests.
 * 
 * @author Erik Wramner
 */
public class QpidJmsConsumer extends JmsConsumer<QpidConsumerConfiguration> {

    /**
     * Program entry point.
     * 
     * @param args Command line.
     * @see QpidConsumerConfiguration
     */
    public static void main(String[] args) {
        new QpidJmsConsumer().run(args);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected QpidConsumerConfiguration createConfiguration() {
        return new QpidConsumerConfiguration();
    }

    /**
     * Apache Qpid consumer configuration.
     * 
     * @author Erik Wramner
     */
    public static class QpidConsumerConfiguration extends JmsConsumerConfiguration {
        @Option(name = "-uri", aliases = {
                        "--jms-uri" }, usage = "AMQP URI for Apache Qpid connection", required = true)
        private String _uri;

        private Context createContext() throws NamingException {
            Hashtable<Object, Object> env = new Hashtable<Object, Object>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
            env.put("connectionfactory.connFactory", _uri);
            if (isDestinationTypeQueue()) {
                env.put("queue." + getDestinationName(), getDestinationName());
            } else {
                env.put("topic." + getDestinationName(), getDestinationName());
            }
            return new InitialContext(env);
        }

        @Override
        public ConnectionFactory createConnectionFactory() throws JMSException {
            try {
                Context context = createContext();
                return (ConnectionFactory) context.lookup("connFactory");
            } catch (NamingException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public XAConnectionFactory createXAConnectionFactory() throws JMSException {
            throw new JMSException("Qpid does not yet support XA transactions");
        }
    }
}
