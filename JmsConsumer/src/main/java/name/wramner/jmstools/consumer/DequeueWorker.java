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
import java.io.IOException;
import java.io.OutputStream;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
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
    }

    protected void processMessages(OutputStream os) throws IOException {
        String jmsId = null;
        String applicationId = null;
        try (ResourceManager resourceManager = _resourceManagerFactory.createResourceManager()) {
            MessageConsumer consumer = resourceManager.getMessageConsumer();

            boolean hasTransaction = false;
            while (_stopController.keepRunning()) {
                if (!hasTransaction) {
                    resourceManager.startTransaction();
                    hasTransaction = true;
                }

                Message msg = _receiveTimeoutMillis > 0 ? consumer.receive(_receiveTimeoutMillis) : consumer
                                .receiveNoWait();
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

                jmsId = msg.getJMSMessageID();
                applicationId = msg.getStringProperty(JMS_PROPNAME_UNIQUE_MESSAGE_ID);

                if (_verifyChecksum) {
                    String md5 = msg.getStringProperty(MessageProvider.CHECKSUM_PROPERTY_NAME);
                    if (md5 == null) {
                        _logger.error("Message with JMS id {} has no checksum property!", jmsId);
                    } else if (msg instanceof TextMessage) {
                        verifyChecksum(new TextMessageData(((TextMessage) msg).getText()), jmsId, applicationId, md5);
                    } else if (msg instanceof BytesMessage) {
                        BytesMessage bytesMessage = (BytesMessage) msg;
                        byte[] payload = new byte[(int) bytesMessage.getBodyLength()];
                        bytesMessage.readBytes(payload);
                        verifyChecksum(new BytesMessageData(payload), jmsId, applicationId, md5);
                    } else {
                        _logger.error("Message {} neither BytesMessage nor TextMessage!", jmsId);
                    }
                }

                if (shouldRollback()) {
                    resourceManager.rollback();
                    _logger.info("Rolled back {} {}", jmsId, applicationId);
                } else {
                    resourceManager.commit();
                    _messageCounter.incrementCount(1);
                    if (os != null) {
                        logMessageIdsToFile(os, jmsId, applicationId);
                    }
                }
                hasTransaction = false;
            }
        } catch (JMSException e) {
            _logger.error("JMS error!", e);
            if (jmsId != null) {
                _logger.info("Rolled back {} {}", jmsId, applicationId);
            }
        } catch (RollbackException | HeuristicRollbackException e) {
            _logger.error("Failed to commit!", e);
            if (jmsId != null) {
                _logger.info("Rolled back {} {}", jmsId, applicationId);
            }
        } catch (HeuristicMixedException e) {
            _logger.error("Failed to commit, but part of the transaction MAY have completed!", e);
            if (jmsId != null) {
                _logger.error("Rolled back OR committed {} {}", jmsId, applicationId);
            }
        } finally {
            jmsId = null;
            applicationId = null;
        }
    }

    private void verifyChecksum(ChecksummedMessageData md, String jmsId, String applicationId, String expectedMd5) {
        if (!md.getChecksum().equals(expectedMd5)) {
            _logger.error("Wrong checksum {} for message with JMS id {} and id {}, expected {}", md.getChecksum(),
                            jmsId, applicationId, expectedMd5);
        }
    }

    private void logMessageIdsToFile(OutputStream os, String jmsMessageId, String messageId) throws IOException {
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(128);
        sb.append(now);
        sb.append('\t');
        sb.append(jmsMessageId);
        sb.append('\t');
        sb.append(messageId);
        sb.append('\n');
        os.write(sb.toString().getBytes());
    }
}