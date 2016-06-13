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

public abstract class ResourceManager implements AutoCloseable {
    private static final Map<String, Queue> QUEUE_MAP = new ConcurrentHashMap<>();
    private MessageProducer _producer;
    private MessageConsumer _consumer;
    protected final String _queueName;

    public ResourceManager(String queueName) {
        _queueName = queueName;
    }

    public MessageProducer getMessageProducer() throws JMSException {
        if (_producer == null) {
            _producer = createMessageProducer();
        }
        return _producer;
    }

    public MessageConsumer getMessageConsumer() throws JMSException {
        if (_consumer == null) {
            _consumer = createMessageConsumer();            
        }
        return _consumer;
    }

    public abstract Session getSession() throws JMSException;

    protected abstract MessageProducer createMessageProducer() throws JMSException;

    protected abstract MessageConsumer createMessageConsumer() throws JMSException;

    public void startTransaction() throws RollbackException, JMSException {
    }

    public void commit() throws JMSException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
    }

    public void rollback() throws JMSException {
    }

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
