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

import name.wramner.jmstools.JmsClientConfiguration;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.BytesMessageProvider;
import name.wramner.jmstools.messages.MessageProvider;
import name.wramner.jmstools.messages.TextMessageProvider;
import name.wramner.jmstools.stopcontroller.CountStopController;
import name.wramner.jmstools.stopcontroller.DurationOrCountStopController;
import name.wramner.jmstools.stopcontroller.DurationStopController;
import name.wramner.jmstools.stopcontroller.RunForeverStopController;
import name.wramner.jmstools.stopcontroller.StopController;

import org.kohsuke.args4j.Option;

/**
 * JMS producer configuration.
 * 
 * @author Erik Wramner
 */
public abstract class JmsProducerConfiguration extends JmsClientConfiguration {
    private static final String DEFAULT_FILE_ENCODING = "UTF-8";
    private static final int DEFAULT_NUMBER_OF_MESSAGES = 100;
    private static final int DEFAULT_MIN_SIZE = 1024;
    private static final int DEFAULT_MAX_SIZE = 8192;
    private static final String DEFAULT_OUTLIER_SIZE = "16M";

    private static enum MessageType {
        TEXT, BYTES
    };

    @Option(name = "-min", aliases = { "--min-message-size" }, usage = "Minimum message size", forbids = { "-dir" })
    protected Integer _minMessageSize;

    @Option(name = "-max", aliases = { "--max-message-size" }, usage = "Maximum message size", forbids = { "-dir" })
    protected Integer _maxMessageSize;

    @Option(name = "-n", aliases = { "--number-of-messages" }, usage = "Number of distinct messages to generate", forbids = { "-dir" })
    protected int _numberOfMessages = DEFAULT_NUMBER_OF_MESSAGES;

    @Option(name = "-dir", aliases = "--message-file-directory", usage = "Directory with files to submit as messages", forbids = {
                    "-min", "-max", "-n", "-outliers", "-outliersize" })
    protected File _messageFileDirectory;

    @Option(name = "-ordered", aliases = "--ordered-delivery", usage = "Send messages in order (works best with one thread)", depends = { "-dir" })
    protected boolean _ordered = false;

    @Option(name = "-enc", aliases = "--message-file-encoding", usage = "Character encoding for message files,"
                    + " relevant for text messages only")
    protected String _messageFileEncoding = DEFAULT_FILE_ENCODING;

    @Option(name = "-id", aliases = "--id-and-checksum", usage = "Set unique id and checksum properties for integrity check")
    protected boolean _idAndChecksumEnabled;

    @Option(name = "-batchsize", aliases = "--messages-per-batch", usage = "Number of messages to send per batch/commit")
    protected int _messagesPerBatch = 1;

    @Option(name = "-sleep", aliases = "--sleep-time-ms", usage = "Sleep time in milliseconds between batches")
    protected int _sleepTimeMillisAfterBatch;

    @Option(name = "-type", aliases = "--message-type", usage = "JMS message type")
    protected JmsProducerConfiguration.MessageType _messageType = MessageType.BYTES;

    @Option(name = "-outliers", aliases = "--outlier-percentage", usage = "Percentage of very large messages, decimals supported", forbids = { "-dir" })
    protected Double _outlierPercentage;

    @Option(name = "-outliersize", aliases = "--outlier-size", usage = "Size of very large messages expressed as bytes (numeric) or "
                    + "with k, M or G suffixes.", forbids = { "-dir" })
    protected String _outlierSize = DEFAULT_OUTLIER_SIZE;

    public Double getOutlierPercentage() {
        return _outlierPercentage;
    }

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

    public int getSleepTimeMillisAfterBatch() {
        return _sleepTimeMillisAfterBatch;
    }

    private Integer getMinMessageSize() {
        return _minMessageSize != null ? _minMessageSize : _maxMessageSize;
    }

    private Integer getMaxMessageSize() {
        return _maxMessageSize != null ? _maxMessageSize : _minMessageSize;
    }

    public boolean isIdAndChecksumEnabled() {
        return _idAndChecksumEnabled;
    }

    public int getMessagesPerBatch() {
        return _messagesPerBatch;
    }

    public StopController createStopController(Counter counter) {
        if (_durationMinutes != null && _stopAfterMessages != null) {
            return new DurationOrCountStopController(_stopAfterMessages.intValue(), counter,
                            _durationMinutes.intValue());
        } else if (_durationMinutes != null) {
            return new DurationStopController(_durationMinutes.intValue());
        } else if (_stopAfterMessages != null) {
            return new CountStopController(_stopAfterMessages.intValue(), counter);
        } else {
            return new RunForeverStopController();
        }
    }

    public MessageProvider createMessageProvider() throws IOException {
        boolean messageTypeText = _messageType == MessageType.TEXT;
        if (_messageFileDirectory != null) {
            String encoding = _messageFileEncoding != null ? _messageFileEncoding : DEFAULT_FILE_ENCODING;
            return messageTypeText ? new TextMessageProvider(_messageFileDirectory,
                            _messageFileEncoding != null ? _messageFileEncoding : DEFAULT_FILE_ENCODING, _ordered)
                            : new BytesMessageProvider(_messageFileDirectory, encoding, _ordered);
        } else if (_minMessageSize != null || _maxMessageSize != null) {
            int minSize = getMinMessageSize();
            int maxSize = getMaxMessageSize();
            int count = Math.min(Math.max(maxSize - minSize, 1), _numberOfMessages);
            return messageTypeText ? new TextMessageProvider(minSize, maxSize, count, _outlierPercentage,
                            getOutlierSizeInBytes()) : new BytesMessageProvider(minSize, maxSize, count,
                            _outlierPercentage, getOutlierSizeInBytes());
        } else {
            return messageTypeText ? new TextMessageProvider(DEFAULT_MIN_SIZE, DEFAULT_MAX_SIZE, _numberOfMessages,
                            _outlierPercentage, getOutlierSizeInBytes()) : new BytesMessageProvider(DEFAULT_MIN_SIZE,
                            DEFAULT_MAX_SIZE, _numberOfMessages, _outlierPercentage, getOutlierSizeInBytes());
        }
    }
}