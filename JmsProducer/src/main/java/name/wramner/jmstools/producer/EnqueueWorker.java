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

import java.io.File;
import java.util.UUID;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;

import name.wramner.jmstools.JmsClientWorker;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.MessageProvider;
import name.wramner.jmstools.rm.ResourceManager;
import name.wramner.jmstools.rm.ResourceManagerFactory;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * An enqueue worker sends test messages provided by a {@link MessageProvider} until a {@link StopController} is
 * satisfied. All messages are logged to a {@link Counter}. The connection may use standard or XA transaction semantics.
 * Messages may be logged with unique identities, making it possible to check if a message has been lost or delivered
 * twice. Receive timeout, sleep times, rollbacks and many other settings are configurable.
 * 
 * @author Erik Wramner
 * @param <T> The configuration class.
 */
public class EnqueueWorker<T extends JmsProducerConfiguration> extends JmsClientWorker<T> {
    private final MessageProvider _messageProvider;
    private final int _messagesPerBatch;
    private final long _sleepTimeMillisAfterBatch;
    private final boolean _idAndChecksumEnabled;
    private final double _delayedDeliveryProbability;
    private final int _delayedDeliverySeconds;
    private final DelayedDeliveryAdapter _delayedDeliveryAdapter;

    /**
     * Constructor.
     * 
     * @param resourceManagerFactory The resource manager factory.
     * @param counter The counter for sent messages.
     * @param stopController The stop controller.
     * @param messageProvider The message provider.
     * @param logFile The log file for sent messages or null.
     * @param config The configuration for other options.
     */
    public EnqueueWorker(ResourceManagerFactory resourceManagerFactory, Counter counter, StopController stopController,
                    MessageProvider messageProvider, File logFile, T config) {
        super(resourceManagerFactory, counter, stopController, logFile, config);
        _messageProvider = messageProvider;
        _messagesPerBatch = config.getMessagesPerBatch();
        _sleepTimeMillisAfterBatch = config.getSleepTimeMillisAfterBatch();
        _idAndChecksumEnabled = config.isIdAndChecksumEnabled();
        if (config.getDelayedDeliveryPercentage() != null) {
            _delayedDeliveryAdapter = config.createDelayedDeliveryAdapter();
            _delayedDeliveryProbability = config.getDelayedDeliveryPercentage().doubleValue() / 100.0;
            _delayedDeliverySeconds = config.getDelayedDeliverySeconds();
        } else {
            _delayedDeliveryAdapter = null;
            _delayedDeliveryProbability = 0.0;
            _delayedDeliverySeconds = 0;
        }
    }

    /**
     * Send messages until the stop controller is satisfied or until an error occurs.
     * 
     * @param resourceManager The resource manager for transaction control.
     * @throws JMSException on JMS errors.
     * @throws RollbackException when the XA resource has been rolled back.
     * @throws HeuristicRollbackException when the XA resource has been rolled back heuristically.
     * @throws HeuristicMixedException when the XA resource has been rolled back OR committed.
     */
    @Override
    protected void processMessages(ResourceManager resourceManager) throws RollbackException, JMSException,
                    HeuristicMixedException, HeuristicRollbackException {
        while (_stopController.keepRunning()) {
            resourceManager.startTransaction();

            for (int i = 0; i < _messagesPerBatch; i++) {
                Message message = _messageProvider.createMessageWithPayloadAndChecksumProperty(resourceManager
                                .getSession());

                if (_idAndChecksumEnabled) {
                    message.setStringProperty(MessageProvider.UNIQUE_MESSAGE_ID_PROPERTY_NAME, UUID.randomUUID()
                                    .toString());
                }

                int delay = 0;
                if (shouldDelayDelivery()) {
                    _delayedDeliveryAdapter.setDelayProperty(message, _delayedDeliverySeconds);
                    delay = _delayedDeliverySeconds;
                }

                if (messageLogEnabled()) {
                    logMessage(message, delay);
                }

                resourceManager.getMessageProducer().send(message);
            }

            commitOrRollback(resourceManager, _messagesPerBatch);
            if (_sleepTimeMillisAfterBatch > 0) {
                _stopController.waitForTimeoutOrDone(_sleepTimeMillisAfterBatch);
            }
        }
    }

    @Override
    protected String[] getMessageLogHeaders() {
        return new String[] { "ProducedTime", "ID", "Length", "DelaySeconds" };
    }

    private void logMessage(Message message, int delay) throws JMSException {
        logMessage(String.valueOf(System.currentTimeMillis()),
                        message.getStringProperty(MessageProvider.UNIQUE_MESSAGE_ID_PROPERTY_NAME),
                        String.valueOf(message.getIntProperty(MessageProvider.LENGTH_PROPERTY_NAME)),
                        String.valueOf(delay));
    }

    private boolean shouldDelayDelivery() {
        return _delayedDeliveryAdapter != null && _random.nextDouble() < _delayedDeliveryProbability;
    }
}
