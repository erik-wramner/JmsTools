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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.MessageProvider;
import name.wramner.jmstools.stopcontroller.StopController;
import name.wramner.util.AutoCloser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnqueueWorker<T extends JmsProducerConfiguration> implements Runnable {
    private static final long JMS_EXCEPTION_RECOVERY_TIME_MS = 10000L;
    private final Logger _logger = LoggerFactory.getLogger(getClass());
    private final Random _random = new Random();
    private final ConnectionFactory _connFactory;
    private final Counter _counter;
    private final StopController _stopController;
    private final MessageProvider _messageProvider;
    private final int _messagesPerBatch;
    private final long _sleepTimeMillisAfterBatch;
    private final boolean _idAndChecksumEnabled;
    private final boolean _rollbacksEnabled;
    private final double _rollbackProbability;
    private final String _queueName;
    private final File _logFile;

    // TODO: message delay
    public EnqueueWorker(ConnectionFactory connFactory, Counter counter, StopController stopController,
                    MessageProvider messageProvider, File logFile, T config) {
        _connFactory = connFactory;
        _counter = counter;
        _stopController = stopController;
        _messageProvider = messageProvider;
        _logFile = logFile;
        _messagesPerBatch = config.getMessagesPerBatch();
        _sleepTimeMillisAfterBatch = config.getSleepTimeMillisAfterBatch();
        _idAndChecksumEnabled = config.isIdAndChecksumEnabled();
        _rollbacksEnabled = config.getRollbackPercentage() != null;
        _rollbackProbability = _rollbacksEnabled ? config.getRollbackPercentage().doubleValue() / 100.0 : 0.0;
        _queueName = config.getQueueName();
    }

    @Override
    public void run() {
        _logger.debug("Enqueue worker starting...");
        OutputStream os = null;
        try {
            if (_idAndChecksumEnabled && _logFile != null) {
                os = new BufferedOutputStream(new FileOutputStream(_logFile));
            }
            Connection conn = null;
            Session session = null;
            MessageProducer producer = null;
            while (_stopController.keepRunning()) {
                try (AutoCloser closer = new AutoCloser(conn = _connFactory.createConnection())) {
                    closer.add(session = conn.createSession(true, Session.SESSION_TRANSACTED));
                    closer.add(producer = session.createProducer(session.createQueue(_queueName)));
                    List<String> messageIds = new ArrayList<>(_messagesPerBatch);

                    while (_stopController.keepRunning()) {
                        messageIds.clear();

                        for (int i = 0; i < _messagesPerBatch; i++) {
                            Message msg = _messageProvider.createMessageWithPayloadAndChecksumProperty(session);

                            if (_idAndChecksumEnabled) {
                                String id = UUID.randomUUID().toString();
                                msg.setStringProperty("Unique-Message-Id", id);
                                messageIds.add(id);
                            }

                            producer.send(msg);
                        }

                        if (shouldRollback()) {
                            session.rollback();
                            if (_idAndChecksumEnabled) {
                                _logger.info("Rolled back {}", messageIds);
                            }
                        } else {
                            session.commit();
                            _counter.incrementCount(_messagesPerBatch);
                            if (os != null) {
                                logMessageIdsToFile(os, messageIds);
                            }
                        }

                        if (_sleepTimeMillisAfterBatch > 0) {
                            Thread.sleep(_sleepTimeMillisAfterBatch);
                        }
                    }
                } catch (JMSException e) {
                    _logger.error("Enqueue worker failed!", e);
                    if (_stopController.keepRunning()) {
                        Thread.sleep(JMS_EXCEPTION_RECOVERY_TIME_MS);
                    }
                }
            }
        } catch (IOException e) {
            _logger.error("Enqueue worker failed with I/O exception!", e);
        } catch (InterruptedException e) {
            _logger.info("Enqueue worker interrupted, aborting!", e);
        } finally {
            if (os != null) {
                flush(os);
                closeSafely(os);
            }
            _logger.debug("Enqueue worker stopped");
        }
    }

    private void closeSafely(OutputStream os) {
        try {
            os.close();
        } catch (IOException e) {
        }
    }

    private void flush(OutputStream os) {
        try {
            os.flush();
        } catch (IOException e) {
        }
    }

    private void logMessageIdsToFile(OutputStream os, List<String> messageIds) throws IOException {
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        for (String id : messageIds) {
            sb.append(now);
            sb.append('\t');
            sb.append(id);
            sb.append('\n');
            os.write(sb.toString().getBytes());
            sb.setLength(0);
        }
    }

    private boolean shouldRollback() {
        return _rollbacksEnabled && _random.nextDouble() < _rollbackProbability;
    }

}