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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;

import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.MessageProvider;
import name.wramner.jmstools.rm.ResourceManager;
import name.wramner.jmstools.rm.ResourceManagerFactory;
import name.wramner.jmstools.stopcontroller.StopController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnqueueWorker<T extends JmsProducerConfiguration> implements Runnable {
    private static final long JMS_EXCEPTION_RECOVERY_TIME_MS = 10000L;
    private final Logger _logger = LoggerFactory.getLogger(getClass());
    private final Random _random = new Random();
    private final Counter _counter;
    private final StopController _stopController;
    private final MessageProvider _messageProvider;
    private final int _messagesPerBatch;
    private final long _sleepTimeMillisAfterBatch;
    private final boolean _idAndChecksumEnabled;
    private final boolean _rollbacksEnabled;
    private final double _rollbackProbability;
    private final double _delayedDeliveryProbability;
    private final int _delayedDeliverySeconds;
    private final File _logFile;
    private final DelayedDeliveryAdapter _delayedDeliveryAdapter;
    private final ResourceManagerFactory _resourceManagerFactory;

    public EnqueueWorker(ResourceManagerFactory resourceManagerFactory, Counter counter, StopController stopController,
                    MessageProvider messageProvider, File logFile, T config) {
        _resourceManagerFactory = resourceManagerFactory;
        _counter = counter;
        _stopController = stopController;
        _messageProvider = messageProvider;
        _logFile = logFile;
        _messagesPerBatch = config.getMessagesPerBatch();
        _sleepTimeMillisAfterBatch = config.getSleepTimeMillisAfterBatch();
        _idAndChecksumEnabled = config.isIdAndChecksumEnabled();
        if (_rollbacksEnabled = config.getRollbackPercentage() != null) {
            _rollbackProbability = config.getRollbackPercentage().doubleValue() / 100.0;
        } else {
            _rollbackProbability = 0.0;
        }
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

    @Override
    public void run() {
        _logger.debug("Enqueue worker starting...");
        OutputStream os = null;
        try {
            if (_idAndChecksumEnabled && _logFile != null) {
                os = new BufferedOutputStream(new FileOutputStream(_logFile));
            }
            List<String> messageIds = new ArrayList<>(_messagesPerBatch);

            while (_stopController.keepRunning()) {
                try (ResourceManager resourceManager = _resourceManagerFactory.createResourceManager()) {
                    while (_stopController.keepRunning()) {
                        resourceManager.startTransaction();
                        messageIds.clear();

                        for (int i = 0; i < _messagesPerBatch; i++) {
                            Message msg = _messageProvider.createMessageWithPayloadAndChecksumProperty(resourceManager
                                            .getSession());
                            if (_idAndChecksumEnabled) {
                                String id = UUID.randomUUID().toString();
                                msg.setStringProperty("Unique-Message-Id", id);
                                messageIds.add(id);
                            }

                            if (shouldDelayDelivery()) {
                                _delayedDeliveryAdapter.setDelayProperty(msg, _delayedDeliverySeconds);
                            }

                            resourceManager.getMessageProducer().send(msg);
                        }

                        if (shouldRollback()) {
                            resourceManager.rollback();
                            if (_idAndChecksumEnabled) {
                                _logger.info("Rolled back {}", messageIds);
                            }
                        } else {
                            resourceManager.commit();
                            _counter.incrementCount(_messagesPerBatch);
                            if (os != null) {
                                logMessageIdsToFile(os, messageIds);
                            }
                        }

                        if (_sleepTimeMillisAfterBatch > 0) {
                            _stopController.waitForTimeoutOrDone(_sleepTimeMillisAfterBatch);
                        }
                    }
                } catch (JMSException e) {
                    _logger.error("JMS error!", e);
                    if (!messageIds.isEmpty()) {
                        _logger.info("Rolled back {}", messageIds);
                    }
                } catch (RollbackException | HeuristicRollbackException e) {
                    _logger.error("Failed to commit!", e);
                    if (!messageIds.isEmpty()) {
                        _logger.info("Rolled back {}", messageIds);
                    }
                } catch (HeuristicMixedException e) {
                    _logger.error("Failed to commit, but part of the transaction MAY have completed!", e);
                    if (!messageIds.isEmpty()) {
                        _logger.error("Rolled back OR committed {}", messageIds);
                    }
                }

                _stopController.waitForTimeoutOrDone(JMS_EXCEPTION_RECOVERY_TIME_MS);
            }
        } catch (Exception e) {
            _logger.error("Enqueue worker failed, aborting!", e);
        } finally {
            if (os != null) {
                flushSafely(os);
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

    private void flushSafely(OutputStream os) {
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

    private boolean shouldDelayDelivery() {
        return _delayedDeliveryAdapter != null && _random.nextDouble() < _delayedDeliveryProbability;
    }
}
