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

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atomikos.icatch.jta.UserTransactionManager;

public class XAJmsResourceManager extends ResourceManager {
    private final Logger _logger = LoggerFactory.getLogger(getClass());
    private final UserTransactionManager _transactionManager;
    private final XAConnectionFactory _connFactory;
    private XAConnection _conn;
    private XASession _session;

    public XAJmsResourceManager(UserTransactionManager transactionManager, XAConnectionFactory connFactory,
                    String queueName) {
        super(queueName);
        _transactionManager = transactionManager;
        _connFactory = connFactory;
    }

    @Override
    protected MessageProducer createMessageProducer() throws JMSException {
        XASession session = getSession();
        return session.createProducer(getQueue(session, _queueName));
    }

    @Override
    protected MessageConsumer createMessageConsumer() throws JMSException {
        XASession session = getSession();
        MessageConsumer consumer = session.createConsumer(getQueue(session, _queueName));
        _conn.start();
        return consumer;
    }

    @Override
    public XASession getSession() throws JMSException {
        if (_session == null) {
            if (_conn == null) {
                _conn = _connFactory.createXAConnection();
            }
            _session = _conn.createXASession();
        }
        return _session;
    }

    @Override
    public void startTransaction() throws RollbackException, JMSException {
        try {
            _transactionManager.begin();
            Transaction tx = _transactionManager.getTransaction();
            tx.enlistResource(getSession().getXAResource());
        } catch (NotSupportedException | SystemException e) {
            _logger.error("Failed to start transaction", e);
            throw new IllegalStateException("Failed to start transaction", e);
        }
    }

    @Override
    public void commit() throws JMSException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
        try {
            Transaction tx = _transactionManager.getTransaction();
            if (tx != null) {
                tx.delistResource(getSession().getXAResource(), XAResource.TMSUCCESS);
                tx.commit();
            }
        } catch (SystemException e) {
            _logger.error("Failed to rollback", e);
            throw new RuntimeException("Failed to rollback", e);
        }
    }

    @Override
    public void rollback() throws JMSException {
        try {
            Transaction tx = _transactionManager.getTransaction();
            if (tx != null) {
                tx.delistResource(getSession().getXAResource(), XAResource.TMFAIL);
                tx.rollback();
            }
        } catch (SystemException e) {
            _logger.error("Failed to rollback", e);
            throw new RuntimeException("Failed to rollback", e);
        }
    }

    @Override
    public void close() {
        super.close();
        try {
            if (_session != null) {
                Transaction tx = _transactionManager.getTransaction();
                if (tx != null) {
                    tx.delistResource(_session.getXAResource(), XAResource.TMFAIL);
                    tx.rollback();
                }
            }
        } catch (SystemException e) {
            // Ignore
        }
        closeSafely(_session);
        closeSafely(_conn);
    }
}
