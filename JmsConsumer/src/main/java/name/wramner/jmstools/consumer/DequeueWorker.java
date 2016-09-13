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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;

import name.wramner.jmstools.JmsClientWorker;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.BytesMessageData;
import name.wramner.jmstools.messages.ChecksummedMessageData;
import name.wramner.jmstools.messages.MessageProvider;
import name.wramner.jmstools.messages.TextMessageData;
import name.wramner.jmstools.rm.ResourceManager;
import name.wramner.jmstools.rm.ResourceManagerFactory;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * A dequeue worker reads and discards messages. It logs messages read and read misses (receive timeouts). It can be
 * configured to rollback a percentage of messages in order to test transaction semantics. It can also verify MD5
 * checksums for messages in order to verify that they have been transferred without alterations. Unique message
 * identities can be logged to file in order to verify that there are no lost or duplicate messages or ghost messages
 * (submitted but then rolled back by a producer).
 *
 * @author Erik Wramner
 */
public class DequeueWorker<T extends JmsConsumerConfiguration> extends JmsClientWorker<T> {
    private final Counter _receiveTimeoutCounter;
    private final int _receiveTimeoutMillis;
    private final int _pollingDelayMillis;
    private final boolean _verifyChecksum;
    private final boolean _shouldCommitOnReceiveTimeout;
    private final File _messageFileDirectory;

    /**
     * Constructor.
     *
     * @param resourceManagerFactory The resource manager factory.
     * @param messageCounter The message counter for dequeued messages.
     * @param receiveTimeoutCounter The counter for receive timeouts.
     * @param stopController The stop controller.
     * @param logFile The log file for received messages or null.
     * @param config The configuration for other options.
     */
    public DequeueWorker(ResourceManagerFactory resourceManagerFactory, Counter messageCounter,
            Counter receiveTimeoutCounter, StopController stopController, File logFile, T config) {
        super(resourceManagerFactory, messageCounter, stopController, logFile, config);
        _receiveTimeoutCounter = receiveTimeoutCounter;
        _receiveTimeoutMillis = config.getReceiveTimeoutMillis();
        _pollingDelayMillis = config.getPollingDelayMillis();
        _verifyChecksum = config.shouldVerifyChecksum();
        _shouldCommitOnReceiveTimeout = config.shouldCommitOnReceiveTimeout();
        _messageFileDirectory = config.getMessageFileDirectory();
    }

    /**
     * Receive messages until the stop controller is satisfied or until an error occurs.
     *
     * @param resourceManager The resource manager for transaction control.
     * @throws JMSException on JMS errors.
     * @throws RollbackException when the XA resource has been rolled back.
     * @throws HeuristicRollbackException when the XA resource has been rolled back heuristically.
     * @throws HeuristicMixedException when the XA resource has been rolled back OR committed.
     */
    @Override
    protected void processMessages(ResourceManager resourceManager)
            throws JMSException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
        MessageConsumer consumer = resourceManager.getMessageConsumer();

        boolean hasTransaction = false;
        while (_stopController.keepRunning()) {
            if (!hasTransaction) {
                resourceManager.startTransaction();
                hasTransaction = true;
            }

            Message msg = _receiveTimeoutMillis > 0 ? consumer.receive(_receiveTimeoutMillis)
                    : consumer.receiveNoWait();
            if (msg == null) {
                _receiveTimeoutCounter.incrementCount(1);
                if (_shouldCommitOnReceiveTimeout) {
                    resourceManager.commit();
                    hasTransaction = false;
                }
                if (_pollingDelayMillis > 0) {
                    _logger.debug("No message, sleeping {} ms", _pollingDelayMillis);
                    _stopController.waitForTimeoutOrDone(_pollingDelayMillis);
                }
                continue;
            }

            String jmsId = msg.getJMSMessageID();
            String applicationId = msg.getStringProperty(MessageProvider.UNIQUE_MESSAGE_ID_PROPERTY_NAME);
            Integer length = null;

            if (_verifyChecksum) {
                String md5 = msg.getStringProperty(MessageProvider.CHECKSUM_PROPERTY_NAME);
                if (md5 == null) {
                    _logger.error("Message with JMS id {} has no checksum property!", jmsId);
                }
                else if (msg instanceof TextMessage) {
                    TextMessageData md = new TextMessageData(((TextMessage) msg).getText());
                    length = md.getLength();
                    verifyChecksum(md, jmsId, applicationId, md5);
                }
                else if (msg instanceof BytesMessage) {
                    BytesMessage bytesMessage = (BytesMessage) msg;
                    byte[] payload = new byte[(int) bytesMessage.getBodyLength()];
                    bytesMessage.readBytes(payload);
                    BytesMessageData md = new BytesMessageData(payload);
                    length = md.getLength();
                    verifyChecksum(md, jmsId, applicationId, md5);
                }
                else {
                    _logger.error("Message {} neither BytesMessage nor TextMessage!", jmsId);
                }
            }

            if (messageLogEnabled()) {
                logMessage(msg, jmsId, applicationId, length);
            }
            if (_messageFileDirectory != null) {
                saveMessage(msg);
            }
            commitOrRollback(resourceManager, 1);
            hasTransaction = false;
        }
    }

    private void saveMessage(Message msg) throws JMSException {
        String baseName = generateUniqueFileName(msg);
        try {
            saveMessagePayload(msg, baseName);
            saveMessageHeaders(msg, baseName);
        }
        catch (IOException e) {
            _logger.error("Failed to save message!", e);
        }
    }

    private void saveMessageHeaders(Message msg, String baseName) throws JMSException, IOException {
        try (FileOutputStream fos = new FileOutputStream(new File(_messageFileDirectory, baseName + ".headers"))) {
            Properties props = new Properties();
            for (Enumeration<?> propertyNames = msg.getPropertyNames(); propertyNames.hasMoreElements();) {
                String name = propertyNames.nextElement().toString();
                props.setProperty(name, msg.getStringProperty(name));
            }
            props.store(fos, "JMS properties for " + msg.getJMSMessageID());
            fos.flush();
        }
    }

    private void saveMessagePayload(Message msg, String baseName) throws JMSException, IOException {
        try (FileOutputStream fos = new FileOutputStream(new File(_messageFileDirectory, baseName + ".payload"))) {
            byte[] payload;
            if (msg instanceof TextMessage) {
                payload = TextMessageData.textToBytes(((TextMessage) msg).getText());
            }
            else if (msg instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) msg;
                payload = new byte[(int) bytesMessage.getBodyLength()];
                bytesMessage.readBytes(payload);
            }
            else if (msg instanceof ObjectMessage) {
                payload = _objectMessageAdapter.getObjectPayload((ObjectMessage) msg);
            }
            else {
                _logger.warn("Can't save payload for {}, unsupported type!", msg.getJMSMessageID());
                payload = new byte[0];
            }
            fos.write(payload);
            fos.flush();
        }
    }

    private String generateUniqueFileName(Message msg) throws JMSException {
        return msg.getJMSMessageID().replace(":", "_");
    }

    private void logMessage(Message msg, String jmsId, String applicationId, Integer length) throws JMSException {
        if (length == null) {
            length = computeMessageLength(msg, length);
        }
        logMessage(String.valueOf(System.currentTimeMillis()), jmsId, applicationId,
            length != null ? length.toString() : null);
    }

    private Integer computeMessageLength(Message msg, Integer length) throws JMSException {
        if (msg instanceof BytesMessage) {
            length = (int) ((BytesMessage) msg).getBodyLength();
        }
        else if (msg instanceof TextMessage) {
            length = TextMessageData.textToBytes(((TextMessage) msg).getText()).length;
        }
        return length;
    }

    private void verifyChecksum(ChecksummedMessageData md, String jmsId, String applicationId, String expectedMd5) {
        if (!md.getChecksum().equals(expectedMd5)) {
            _logger.error("Wrong checksum {} for message with JMS id {} and id {}, expected {}", md.getChecksum(),
                jmsId, applicationId, expectedMd5);
        }
    }

    @Override
    protected String[] getMessageLogHeaders() {
        return new String[] { "ConsumedTime", "JMSID", "ID", "Length" };
    }
}