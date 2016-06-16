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
package name.wramner.jmstools.rm;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;

/**
 * A resource manager manages resources. Yeah, right. The name is awful. To be more specific a resource manager in this
 * context manages the JMS resources for a producer or consumer as well as the transaction (XA or local) for the same.
 * 
 * @author Erik Wramner
 */
public abstract class ResourceManager implements AutoCloseable {
    private static final Map<String, Queue> QUEUE_MAP = new ConcurrentHashMap<>();
    private MessageProducer _producer;
    private MessageConsumer _consumer;
    protected final String _queueName;

    /**
     * Constructor.
     * 
     * @param queueName The queue name.
     */
    public ResourceManager(String queueName) {
        _queueName = queueName;
    }

    /**
     * Get message producer, create if necessary.
     * 
     * @return producer.
     * @throws JMSException on errors.
     */
    public MessageProducer getMessageProducer() throws JMSException {
        if (_producer == null) {
            _producer = createMessageProducer();
        }
        return _producer;
    }

    /**
     * Get message consumer, create if necessary. Also start connection.
     * 
     * @return consumer.
     * @throws JMSException on errors.
     */
    public MessageConsumer getMessageConsumer() throws JMSException {
        if (_consumer == null) {
            _consumer = createMessageConsumer();
        }
        return _consumer;
    }

    /**
     * Get JMS session.
     * 
     * @return session.
     * @throws JMSException on errors.
     */
    public abstract Session getSession() throws JMSException;

    /**
     * Create a message producer.
     * 
     * @return new producer.
     * @throws JMSException on errors.
     */
    protected abstract MessageProducer createMessageProducer() throws JMSException;

    /**
     * Create a message consumer.
     * 
     * @return new consumer.
     * @throws JMSException on errors.
     */
    protected abstract MessageConsumer createMessageConsumer() throws JMSException;

    /**
     * Start a new transaction. The default implementation does nothing, but for XA transactions this is needed.
     * 
     * @throws RollbackException if rolled back.
     * @throws JMSException on JMS errors.
     */
    public void startTransaction() throws RollbackException, JMSException {
    }

    /**
     * Commit transaction.
     * 
     * @throws JMSException on JMS errors.
     * @throws RollbackException if rolled back.
     * @throws HeuristicMixedException if partly committed and partly rolled back.
     * @throws HeuristicRollbackException if rolled back.
     */
    public abstract void commit() throws JMSException, RollbackException, HeuristicMixedException,
                    HeuristicRollbackException;

    /**
     * Roll back transaction.
     * 
     * @throws JMSException on JMS errors.
     */
    public abstract void rollback() throws JMSException;

    /**
     * Close resources.
     * 
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() {
        closeSafely(_consumer);
        closeSafely(_producer);
    }

    /**
     * Get cached queue or create and return it.
     * 
     * @param session The JMS session.
     * @param queueName The queue name.
     * @return queue.
     * @throws JMSException on errors.
     */
    protected static Queue getQueue(Session session, String queueName) throws JMSException {
        Queue queue = QUEUE_MAP.get(queueName);
        if (queue == null) {
            queue = session.createQueue(queueName);
            QUEUE_MAP.put(queueName, queue);
        }
        return queue;
    }

    /**
     * Close the object and ignore any exceptions.
     * 
     * @param o The object.
     * @throws IllegalStateException if there is no close method.
     */
    protected static void closeSafely(Object o) {
        if (o != null) {
            if (o instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) o).close();
                } catch (Exception e) {
                    // Ignore exception
                }
            } else if (o instanceof Closeable) {
                try {
                    ((Closeable) o).close();
                } catch (IOException e) {
                    // Ignore exception
                }
            } else {
                Method closeMethod;
                try {
                    closeMethod = o.getClass().getMethod("close");
                } catch (NoSuchMethodException e) {
                    throw new IllegalStateException("Not possible to clean up " + o.getClass().getName()
                                    + ": no close method!", e);
                }
                try {
                    closeMethod.invoke(o);
                } catch (Exception e) {
                    // Ignore all exceptions
                }
            }
        }
    }
}
