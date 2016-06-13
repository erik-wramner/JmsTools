package name.wramner.jmstools.rm;

import javax.jms.XAConnectionFactory;

import com.atomikos.icatch.jta.UserTransactionManager;

public class XAJmsResourceManagerFactory implements ResourceManagerFactory {
    private final UserTransactionManager _transactionManager;
    private final XAConnectionFactory _connFactory;
    private final String _queueName;

    public XAJmsResourceManagerFactory(UserTransactionManager transactionManager, XAConnectionFactory connFactory,
                    String queueName) {
        _transactionManager = transactionManager;
        _connFactory = connFactory;
        _queueName = queueName;
    }

    @Override
    public ResourceManager createResourceManager() {
        return new XAJmsResourceManager(_transactionManager, _connFactory, _queueName);
    }

}
