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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class JmsResourceManager extends ResourceManager {
    private final ConnectionFactory _connFactory;
    private Connection _conn;
    private Session _session;

    public JmsResourceManager(ConnectionFactory connFactory, String queueName) {
        super(queueName);
        _connFactory = connFactory;
    }

    @Override
    protected MessageProducer createMessageProducer() throws JMSException {
        Session session = getSession();
        return session.createProducer(getQueue(session, _queueName));
    }

    @Override
    protected MessageConsumer createMessageConsumer() throws JMSException {
        Session session = getSession();
        MessageConsumer consumer = session.createConsumer(getQueue(session, _queueName));
        _conn.start();
        return consumer;
    }

    @Override
    public Session getSession() throws JMSException {
        if (_session == null) {
            if (_conn == null) {
                _conn = _connFactory.createConnection();
            }
            _session = _conn.createSession(true, Session.SESSION_TRANSACTED);
        }
        return _session;
    }

    @Override
    public void startTransaction() {
        // Only for XA transactions
    }

    @Override
    public void commit() throws JMSException {
        if (_session != null) {
            _session.commit();
        }
    }

    @Override
    public void rollback() throws JMSException {
        if (_session != null) {
            _session.rollback();
        }
    }

    @Override
    public void close() {
        super.close();
        closeSafely(_session);
        closeSafely(_conn);
    }
}
