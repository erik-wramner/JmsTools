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
package name.wramner.jmstools.messages;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * A message provider initializes messages that can be sent. Messages can be read from a file/directory or generated
 * randomly.
 * 
 * @author Erik Wramner
 *
 * @param <T> The message data type.
 */
public abstract class BaseMessageProvider<T extends ChecksummedMessageData> implements MessageProvider {
    private static final String CHECKSUM_PROPERTY_NAME = "Payload-Checksum-MD5";
    protected final Random _random = new Random();
    private final List<T> _messageDataList = new ArrayList<>();
    private final boolean _ordered;
    private final AtomicInteger _messageIndex = new AtomicInteger(0);
    private final Double _outlierPercentage;
    private final int _outlierSize;

    /**
     * Constructor for prepared messages.
     * 
     * @param fileOrDirectory The single file or the directory containing files.
     * @param encoding The character encoding.
     * @param ordered The flag to send messages in order or randomly.
     * @throws IOException on read errors.
     */
    protected BaseMessageProvider(File fileOrDirectory, String encoding, boolean ordered) throws IOException {
        if (fileOrDirectory.isDirectory()) {
            File[] files = fileOrDirectory.listFiles();
            Arrays.sort(files);
            for (File f : files) {
                if (f.isFile()) {
                    _messageDataList.add(createMessageData(Files.readAllBytes(f.toPath()), Charset.forName(encoding)));
                }
            }
        } else {
            _messageDataList.add(createMessageData(Files.readAllBytes(fileOrDirectory.toPath()),
                            Charset.forName(encoding)));
        }
        _ordered = ordered;
        _outlierPercentage = null;
        _outlierSize = 0;
    }

    /**
     * Constructor for messages with a given size range.
     * 
     * @param minSize The minimum size.
     * @param maxSize The maximum size.
     * @param numberOfMessages The number of messages to generate.
     * @param outlierPercentage The outlier percentage or null.
     * @param outlierSize The outlier size.
     */
    protected BaseMessageProvider(int minSize, int maxSize, int numberOfMessages, Double outlierPercentage,
                    int outlierSize) {
        if (maxSize < minSize) {
            throw new IllegalArgumentException("Max size must be >= min size");
        }
        int step = (maxSize - minSize) / numberOfMessages;
        if (step == 0 && maxSize > minSize) {
            step = 1;
        }
        int size = minSize;
        for (int i = 0; i < numberOfMessages; i++) {
            _messageDataList.add(createRandomMessageData(size));
            if (size < maxSize) {
                size += step;
            }
        }
        _ordered = false;
        _outlierPercentage = outlierPercentage;
        _outlierSize = outlierSize;
    }

    /**
     * Create random message data with a given size.
     * 
     * @param size The number of bytes or characters for the message.
     * @return message data.
     */
    protected abstract T createRandomMessageData(int size);

    /**
     * Create message data for a byte array using a given encoding. The encoding is only relevant for text messages.
     * 
     * @param bytes The bytes.
     * @param characterEncoding The encoding for text messages.
     * @return message data.
     */
    protected abstract T createMessageData(byte[] bytes, Charset characterEncoding);

    /**
     * Create a JMS message for the specified session with prepared payload.
     * 
     * @param session The session.
     * @return message.
     * @throws JMSException on errors.
     */
    protected abstract Message createMessageWithPayload(Session session, T messageData) throws JMSException;

    /**
     * Create a JMS message for the specified session with prepared payload and a checksum.
     * 
     * @param session The session.
     * @return message.
     * @throws JMSException on errors.
     */
    @Override
    public Message createMessageWithPayloadAndChecksumProperty(Session session) throws JMSException {
        T messageData;
        if (_outlierPercentage != null && _random.nextDouble() < (_outlierPercentage.doubleValue() / 100.0)) {
            messageData = createRandomMessageData(_outlierSize);
        } else {
            messageData = getNextMessageData();
        }
        Message msg = createMessageWithPayload(session, messageData);
        msg.setStringProperty(CHECKSUM_PROPERTY_NAME, messageData.getChecksum());
        return msg;
    }

    /**
     * Get data for the next message, either in order (when using prepared files, most useful with a single thread) or
     * randomly.
     * 
     * @return message data.
     */
    public T getNextMessageData() {
        int numberOfMessages = _messageDataList.size();
        int index = _ordered ? _messageIndex.incrementAndGet() % numberOfMessages : _random.nextInt(numberOfMessages);
        return _messageDataList.get(index);
    }
}