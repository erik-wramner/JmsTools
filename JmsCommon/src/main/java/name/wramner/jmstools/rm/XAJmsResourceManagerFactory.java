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

import javax.jms.XAConnectionFactory;

import com.atomikos.icatch.jta.UserTransactionManager;

/**
 * Factory for XA JMS {@link ResourceManager}.
 * 
 * @author Erik Wramner
 */
public class XAJmsResourceManagerFactory implements ResourceManagerFactory {
    private final UserTransactionManager _transactionManager;
    private final XAConnectionFactory _connFactory;
    private final String _queueName;

    /**
     * Constructor.
     * 
     * @param transactionManager The transaction manager.
     * @param connFactory The XA connection factory.
     * @param queueName The queue name.
     */
    public XAJmsResourceManagerFactory(UserTransactionManager transactionManager, XAConnectionFactory connFactory,
                    String queueName) {
        _transactionManager = transactionManager;
        _connFactory = connFactory;
        _queueName = queueName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceManager createResourceManager() {
        return new XAJmsResourceManager(_transactionManager, _connFactory, _queueName);
    }

}
