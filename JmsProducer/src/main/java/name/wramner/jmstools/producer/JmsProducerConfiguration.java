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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import name.wramner.jmstools.JmsClientConfiguration;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.BytesMessageProvider;
import name.wramner.jmstools.messages.MessageProvider;
import name.wramner.jmstools.messages.ObjectMessageProvider;
import name.wramner.jmstools.messages.TextMessageProvider;
import name.wramner.jmstools.stopcontroller.CountStopController;
import name.wramner.jmstools.stopcontroller.DurationOrCountStopController;
import name.wramner.jmstools.stopcontroller.DurationStopController;
import name.wramner.jmstools.stopcontroller.RunForeverStopController;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * JMS producer configuration.
 *
 * @author Erik Wramner
 */
public abstract class JmsProducerConfiguration extends JmsClientConfiguration {
    private static final int ONE_MINUTE_MS = 60_000;
    private static final int AVERAGE_PROCESSING_TIME_MS = 1;
    private static final String DEFAULT_FILE_ENCODING = "UTF-8";
    private static final int DEFAULT_NUMBER_OF_MESSAGES = 100;
    private static final int DEFAULT_MIN_SIZE = 1024;
    private static final int DEFAULT_MAX_SIZE = 8192;
    private static final String DEFAULT_OUTLIER_SIZE = "16M";

    private static enum MessageType {
        TEXT, BYTES, OBJECT
    };

    @Option(name = "-min", aliases = { "--min-message-size" }, usage = "Minimum message size", forbids = { "-dir" })
    protected Integer _minMessageSize;

    @Option(name = "-max", aliases = { "--max-message-size" }, usage = "Maximum message size", forbids = { "-dir" })
    protected Integer _maxMessageSize;

    @Option(name = "-n", aliases = {
                    "--number-of-messages" }, usage = "Number of distinct messages to generate", forbids = { "-dir" })
    protected int _numberOfMessages = DEFAULT_NUMBER_OF_MESSAGES;

    @Option(name = "-dir", aliases = "--message-file-directory", usage = "Directory with files to submit as messages", forbids = {
                    "-min", "-max", "-n", "-outliers", "-outliersize", "-d" })
    protected File _messageFileDirectory;

    @Option(name = "-ordered", aliases = "--ordered-delivery", usage = "Send messages in order (works best with one thread)", depends = {
                    "-dir" })
    protected boolean _ordered = false;

    @Option(name = "-enc", aliases = "--message-file-encoding", usage = "Character encoding for message files,"
                    + " relevant for text messages only")
    protected String _messageFileEncoding = DEFAULT_FILE_ENCODING;

    @Option(name = "-id", aliases = "--id-and-checksum", usage = "Set unique id, length and checksum properties for integrity check")
    protected boolean _idAndChecksumEnabled;

    @Option(name = "-batchsize", aliases = "--messages-per-batch", usage = "Number of messages to send per batch/commit")
    protected int _messagesPerBatch = 1;

    @Option(name = "-sleep", aliases = "--sleep-time-ms", usage = "Sleep time in milliseconds between batches")
    private Integer _initialSleepTimeMillisAfterBatch;

    private AtomicInteger _sleepTimeMillisAfterBatch;

    @Option(name = "-tpm", aliases = "--messages-per-minute", usage = "Target for number of transactions/messages per minute")
    private Integer _targetTpm;

    @Option(name = "-type", aliases = "--message-type", usage = "JMS message type")
    private JmsProducerConfiguration.MessageType _messageType;

    @Option(name = "-outliers", aliases = "--outlier-percentage", usage = "Percentage of very large messages, decimals supported", forbids = {
                    "-dir" })
    protected Double _outlierPercentage;

    @Option(name = "-outliersize", aliases = "--outlier-size", usage = "Size of very large messages expressed as bytes (numeric) or "
                    + "with k, M or G suffixes.", forbids = { "-dir" })
    protected String _outlierSize = DEFAULT_OUTLIER_SIZE;

    @Option(name = "-delay-pct", aliases = "--delayed-delivery-percentage", usage = "Percentage of delayed (scheduled) messages")
    protected Double _delayedDeliveryPercentage;

    @Option(name = "-delay-sec", aliases = "--delayed-delivery-seconds", usage = "The number of seconds to delay scheduled messages")
    protected int _delayedDeliverySeconds;

    @Option(name = "-ttl", aliases = "--time-to-live-millis", usage = "The number of milliseconds before a message expires")
    protected Long _timeToLiveMillis;

    @Option(name = "-h", aliases = "--headers", usage = "JMS headers, \"header1=value1 header2=value2...\"", handler = StringArrayOptionHandler.class)
    private List<String> _headers = new ArrayList<>();

    @Option(name = "-d", aliases = "--data", usage = "Message content (plain text)", forbids = { "-dir" })
    private String _data;

    @Option(name = "-nopersist", aliases = "--non-persistent-delivery", usage = "Use non-persistent delivery. Fast, but expect lost messages.", forbids = {
                    "-xa" })
    private boolean _nonPersistentDelivery;


    /**
     * Should messages be sent with non-persistent delivery mode?
     *
     * @return true or false.
     */
    public boolean isNonPersistentDeliveryRequested() {
        return _nonPersistentDelivery;
    }

    /**
     * Get the message type.
     *
     * @return message type.
     */
    public MessageType getMessageType() {
        if (_messageType != null) {
            return _messageType;
        }
        if (_data != null) {
            return MessageType.TEXT;
        }
        return MessageType.BYTES;
    }

    /**
     * Get a map with keys and values to use as JMS headers in addition to headers generated by other options.
     *
     * @return map with JMS key-value pairs.
     */
    public Map<String, String> getHeaderMap() {
        Map<String, String> headerMap = new TreeMap<>();
        for (String headerKeyValuePair : _headers) {
            int pos = headerKeyValuePair.indexOf('=');
            if (pos != -1) {
                int endIndexForKey = pos - 1;
                int startIndexForValue = pos + 1;
                headerMap.put(headerKeyValuePair.substring(0, endIndexForKey),
                                startIndexForValue < headerKeyValuePair.length()
                                                ? headerKeyValuePair.substring(startIndexForValue)
                                                : null);
            } else {
                headerMap.put(headerKeyValuePair, null);
            }
        }
        return headerMap;
    }

    /**
     * Get the percentage of delayed messages.
     *
     * @return percentage or null for none.
     */
    public Double getDelayedDeliveryPercentage() {
        return _delayedDeliveryPercentage;
    }

    /**
     * Get the delay time in seconds for delayed messages.
     *
     * @return delay time.
     */
    public int getDelayedDeliverySeconds() {
        return _delayedDeliverySeconds;
    }

    /**
     * Get the time to live for posted messages in milliseconds.
     *
     * @return ttl or null for no expiration.
     */
    public Long getTimeToLiveMillis() {
        return _timeToLiveMillis;
    }

    /**
     * Get the target for messages per minute or null if not configured.
     *
     * @return desired number of messages per minute.
     */
    public Integer getTargetTpm() {
        return _targetTpm;
    }

    /**
     * Get the outlier size in bytes. An outlier is a message much larger than the normal message size.
     *
     * @return outlier size in bytes.
     */
    public Integer getOutlierSizeInBytes() {
        if (_outlierSize != null) {
            if (_outlierSize.endsWith("G")) {
                return 1024 * 1024 * 1024 * Integer.parseInt(_outlierSize.substring(0, _outlierSize.length() - 1));
            } else if (_outlierSize.endsWith("M")) {
                return 1024 * 1024 * Integer.parseInt(_outlierSize.substring(0, _outlierSize.length() - 1));
            } else if (_outlierSize.endsWith("k")) {
                return 1024 * Integer.parseInt(_outlierSize.substring(0, _outlierSize.length() - 1));
            } else {
                return Integer.parseInt(_outlierSize);
            }
        }
        return null;
    }

    /**
     * Get the sleep time in milliseconds after a batch (a commit).
     *
     * @return sleep time.
     */
    public synchronized AtomicInteger getSleepTimeMillisAfterBatch() {
        if (_sleepTimeMillisAfterBatch == null) {
            int initialSleepMillis;
            if (_initialSleepTimeMillisAfterBatch == null) {
                if (_targetTpm != null && _targetTpm.intValue() > 0) {
                    int messagesPerMinute = _targetTpm.intValue();
                    initialSleepMillis = Math.max(
                                    (getThreads() * ONE_MINUTE_MS - (messagesPerMinute * AVERAGE_PROCESSING_TIME_MS))
                                                    / messagesPerMinute,
                                    0) * getMessagesPerBatch();
                } else {
                    initialSleepMillis = 0;
                }
            } else {
                initialSleepMillis = _initialSleepTimeMillisAfterBatch.intValue();
            }
            _sleepTimeMillisAfterBatch = new AtomicInteger(initialSleepMillis);
        }
        return _sleepTimeMillisAfterBatch;
    }

    /**
     * Get the minimum message size for random messages.
     *
     * @return size.
     */
    private Integer getMinMessageSize() {
        return _minMessageSize != null ? _minMessageSize : _maxMessageSize;
    }

    /**
     * Get the maximum message size for random messages.
     *
     * @return size.
     */
    private Integer getMaxMessageSize() {
        return _maxMessageSize != null ? _maxMessageSize : _minMessageSize;
    }

    /**
     * Check if id and checksum is enabled. If so every message is enriched with a unique GUID and a MD5 checksum.
     *
     * @return true if messages should be stamped with id and checksum.
     */
    public boolean isIdAndChecksumEnabled() {
        return _idAndChecksumEnabled;
    }

    /**
     * Get the number of messages to send per commit.
     *
     * @return number of messages per commit (batch size).
     */
    public int getMessagesPerBatch() {
        return _messagesPerBatch;
    }

    /**
     * Create a stop controller based on the configuration options.
     *
     * @param counter The message counter.
     * @return stop controller.
     */
    public StopController createStopController(Counter counter) {
        if (_durationMinutes != null && _stopAfterMessages != null) {
            return new DurationOrCountStopController(_stopAfterMessages.intValue(), counter,
                            _durationMinutes.intValue());
        } else if (_durationMinutes != null) {
            return new DurationStopController(_durationMinutes.intValue());
        } else if (_stopAfterMessages != null) {
            return new CountStopController(_stopAfterMessages.intValue(), counter);
        } else if (_messageFileDirectory != null && _ordered) {
            return new CountStopController(countPreparedFiles(), counter);
        } else {
            return new RunForeverStopController();
        }
    }

    /**
     * Create a message provider based on the configuration options.
     *
     * @return message provider.
     * @throws IOException on failure to read prepared messages.
     */
    public MessageProvider createMessageProvider() throws IOException {
        if (_data != null) {
            return new TextMessageProvider(_data, getHeaderMap(), _durationMinutes == null
                            && (_stopAfterMessages == null || _stopAfterMessages.intValue() == 1));
        } else if (_messageFileDirectory != null) {
            boolean noDuplicates = _ordered && _durationMinutes == null
                            && (_stopAfterMessages == null || _stopAfterMessages.intValue() == countPreparedFiles());
            String encoding = _messageFileEncoding != null ? _messageFileEncoding : DEFAULT_FILE_ENCODING;
            switch (getMessageType()) {
            case TEXT:
                return new TextMessageProvider(_messageFileDirectory, encoding, getHeaderMap(), _ordered, noDuplicates);
            case BYTES:
                return new BytesMessageProvider(_messageFileDirectory, encoding, getHeaderMap(), _ordered,
                                noDuplicates);
            case OBJECT:
                return new ObjectMessageProvider(_messageFileDirectory, getObjectMessageAdapter(), getHeaderMap(),
                                _ordered, noDuplicates);
            default:
                throw new IllegalStateException("Message type " + _messageType + " not handled!");
            }
        } else if (_minMessageSize != null || _maxMessageSize != null) {
            int minSize = getMinMessageSize();
            int maxSize = getMaxMessageSize();
            int count = Math.min(Math.max(maxSize - minSize, 1), _numberOfMessages);
            return createMessageProviderForRandomData(minSize, maxSize, count);
        } else {
            return createMessageProviderForRandomData(DEFAULT_MIN_SIZE, DEFAULT_MAX_SIZE, _numberOfMessages);
        }
    }

    /**
     * Create an adapter for delayed message delivery. This is provider specific, so at this level null will always be
     * returned.
     *
     * @return null.
     */
    public DelayedDeliveryAdapter createDelayedDeliveryAdapter() {
        return null;
    }

    private MessageProvider createMessageProviderForRandomData(int minSize, int maxSize, int count) {
        switch (getMessageType()) {
        case TEXT:
            return new TextMessageProvider(minSize, maxSize, count, _outlierPercentage, getOutlierSizeInBytes(),
                            getHeaderMap());
        case BYTES:
            return new BytesMessageProvider(minSize, maxSize, count, _outlierPercentage, getOutlierSizeInBytes(),
                            getHeaderMap());
        case OBJECT:
            throw new IllegalStateException("Object messages with random data not supported!");
        default:
            throw new IllegalStateException("Message type " + _messageType + " not handled!");
        }
    }

    private int countPreparedFiles() {
        int numberOfFiles = 0;
        if (_messageFileDirectory != null) {
            if (_messageFileDirectory.isDirectory()) {
                File[] files = _messageFileDirectory.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isFile() && !file.getName().endsWith(".headers")) {
                            numberOfFiles++;
                        }
                    }
                }
            } else {
                numberOfFiles = 1;
            }
        }
        return numberOfFiles;
    }
}
