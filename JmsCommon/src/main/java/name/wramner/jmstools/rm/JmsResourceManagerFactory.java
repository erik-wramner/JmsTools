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

import javax.jms.ConnectionFactory;

/**
 * Factory for standard non-XA JMS {@link ResourceManager}.
 *
 * @author Erik Wramner
 */
public class JmsResourceManagerFactory implements ResourceManagerFactory {
    private final ConnectionFactory _connFactory;
    private final String _destinationName;
    private final boolean _destinationTypeQueue;
    private final boolean _transaction;

    /**
     * Constructor.
     *
     * @param connFactory The JMS connection factory.
     * @param destinationName The destination name.
     * @param destinationTypeQueue The flag selecting queue or topic.
     * @param transaction The flag to use transactions.
     */
    public JmsResourceManagerFactory(ConnectionFactory connFactory, String destinationName,
            boolean destinationTypeQueue, boolean transaction) {
        _connFactory = connFactory;
        _destinationName = destinationName;
        _destinationTypeQueue = destinationTypeQueue;
        _transaction = transaction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResourceManager createResourceManager() {
        return new JmsResourceManager(_connFactory, _destinationName, _destinationTypeQueue, _transaction);
    }
}
