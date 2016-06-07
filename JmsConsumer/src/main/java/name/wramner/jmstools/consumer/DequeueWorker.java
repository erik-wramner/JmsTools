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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.BytesMessageData;
import name.wramner.jmstools.messages.TextMessageData;
import name.wramner.jmstools.stopcontroller.StopController;
import name.wramner.util.AutoCloser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A dequeue worker reads and discards messages. It logs messages read and read misses (receive timeouts). It can be
 * configured to rollback a percentage of messages in order to test transaction semantics. It can also verify MD5
 * checksums for messages in order to verify that they have been transferred without alterations. Unique message
 * identities can be logged to file in order to verify that there are no lost or duplicate messages or ghost messages
 * (submitted but then rolled back by a producer).
 * 
 * @author Erik Wramner
 */
public class DequeueWorker<T extends JmsConsumerConfiguration> implements Runnable {
    private static final long JMS_EXCEPTION_RECOVERY_TIME_MS = 10000L;
    private final Logger _logger = LoggerFactory.getLogger(getClass());
    private final Random _random = new Random();
    private final ConnectionFactory _connFactory;
    private final Counter _messageCounter;
    private final Counter _receiveTimeoutCounter;
    private final StopController _stopController;
    private final boolean _rollbacksEnabled;
    private final double _rollbackProbability;
    private final String _queueName;
    private final File _logFile;
    private final int _receiveTimeoutMillis;
    private final int _pollingDelayMillis;
    private final boolean _verifyChecksum;

    /**
     * Constructor.
     * 
     * @param connFactory The JMS connection factory.
     * @param messageCounter The message counter for dequeued messages.
     * @param receiveTimeoutCounter The counter for receive timeouts.
     * @param stopController The stop controller.
     * @param logFile The log file for received messages or null.
     * @param config The configuration for other options.
     */
    public DequeueWorker(ConnectionFactory connFactory, Counter messageCounter, Counter receiveTimeoutCounter,
                    StopController stopController, File logFile, T config) {
        _connFactory = connFactory;
        _messageCounter = messageCounter;
        _receiveTimeoutCounter = receiveTimeoutCounter;
        _stopController = stopController;
        _logFile = logFile;
        _rollbacksEnabled = config.getRollbackPercentage() != null;
        _rollbackProbability = _rollbacksEnabled ? config.getRollbackPercentage().doubleValue() / 100.0 : 0.0;
        _queueName = config.getQueueName();
        _receiveTimeoutMillis = config.getReceiveTimeoutMillis();
        _pollingDelayMillis = config.getPollingDelayMillis();
        _verifyChecksum = config.shouldVerifyChecksum();
    }

    /**
     * Dequeue messages until the stop controller says time is out or until a non-JMS error occurs.
     */
    @Override
    public void run() {
        _logger.debug("Dequeue worker starting...");
        OutputStream os = null;
        try {
            if (_logFile != null) {
                os = new BufferedOutputStream(new FileOutputStream(_logFile));
            }
            Connection conn = null;
            Session session = null;
            MessageConsumer consumer = null;
            while (_stopController.keepRunning()) {
                try (AutoCloser closer = new AutoCloser(conn = _connFactory.createConnection())) {
                    closer.add(session = conn.createSession(true, Session.SESSION_TRANSACTED));
                    closer.add(consumer = session.createConsumer(session.createQueue(_queueName)));
                    conn.start();

                    while (_stopController.keepRunning()) {
                        Message msg = _receiveTimeoutMillis > 0 ? consumer.receive(_receiveTimeoutMillis) : consumer
                                        .receiveNoWait();
                        if (msg == null) {
                            _receiveTimeoutCounter.incrementCount(1);
                            if (_pollingDelayMillis > 0) {
                                _logger.debug("No message, sleeping {} ms", _pollingDelayMillis);
                                Thread.sleep(_pollingDelayMillis);
                            }
                            continue;
                        }

                        String jmsId = msg.getJMSMessageID();

                        if (_verifyChecksum) {
                            String md5 = msg.getStringProperty("Payload-Checksum-MD5");
                            if (md5 == null) {
                                _logger.error("Message with JMS id {} has no checksum property!", jmsId);
                            } else if (msg instanceof TextMessage) {
                                TextMessageData md = new TextMessageData(((TextMessage) msg).getText());
                                if (!md.getChecksum().equals(md5)) {
                                    _logger.error("Wrong checksum {} for message with JMS id {}, expected {}",
                                                    md.getChecksum(), jmsId, md5);
                                }
                            } else if (msg instanceof BytesMessage) {
                                BytesMessage bytesMessage = (BytesMessage) msg;
                                byte[] payload = new byte[(int) bytesMessage.getBodyLength()];
                                bytesMessage.readBytes(payload);
                                BytesMessageData md = new BytesMessageData(payload);
                                if (!md.getChecksum().equals(md5)) {
                                    _logger.error("Wrong checksum {} for message with JMS id {}, expected {}",
                                                    md.getChecksum(), jmsId, md5);
                                }
                            } else {
                                _logger.error("Message {} neither BytesMessage nor TextMessage!", jmsId);
                            }
                        }

                        if (shouldRollback()) {
                            session.rollback();
                            _logger.debug("Rolled back {}", jmsId);
                        } else {
                            session.commit();
                            _messageCounter.incrementCount(1);
                            if (os != null) {
                                logMessageIdsToFile(os, jmsId, msg.getStringProperty("Unique-Message-Id"));
                            }
                        }
                    }
                } catch (JMSException e) {
                    _logger.error("Dequeue worker failed!", e);
                    if (_stopController.keepRunning()) {
                        Thread.sleep(JMS_EXCEPTION_RECOVERY_TIME_MS);
                    }
                }
            }
        } catch (IOException e) {
            _logger.error("Dequeue worker failed with I/O exception!", e);
        } catch (InterruptedException e) {
            _logger.info("Dequeue worker interrupted, aborting!", e);
        } finally {
            if (os != null) {
                try {
                    os.flush();
                    os.close();
                } catch (IOException e) {
                }
            }
            _logger.debug("Dequeue worker stopped");
        }
    }

    private void logMessageIdsToFile(OutputStream os, String jmsMessageId, String messageId) throws IOException {
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        sb.append(now);
        sb.append('\t');
        sb.append(jmsMessageId);
        sb.append('\t');
        sb.append(messageId);
        sb.append('\n');
        os.write(sb.toString().getBytes());
    }

    private boolean shouldRollback() {
        return _rollbacksEnabled && _random.nextDouble() < _rollbackProbability;
    }
}