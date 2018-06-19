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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * A message provider initializes messages that can be sent. Messages can be read from a file/directory or generated
 * randomly. When messages are read from the file system, prepared JMS headers may be read as well. A file that ends
 * with &quot;.payload&quot; is assumed to correspond to a file with the same base name and the suffix
 * &quot;.headers&quot;.
 *
 * @author Erik Wramner
 *
 * @param <T> The message data type.
 */
public abstract class BaseMessageProvider<T extends ChecksummedMessageData> implements MessageProvider {
    protected final Random _random = new Random();
    private final List<T> _messageDataList = new ArrayList<>();
    private final List<Map<String, String>> _messageHeaderList = new ArrayList<>();
    private final boolean _ordered;
    private final boolean _noDuplicates;
    private final AtomicInteger _messageIndex = new AtomicInteger(0);
    private final Double _outlierPercentage;
    private final int _outlierSize;

    /**
     * Constructor for prepared messages.
     *
     * @param fileOrDirectory The single file or the directory containing files.
     * @param encoding The character encoding.
     * @param ordered The flag to send messages in order or randomly.
     * @param noDuplicates The flag to stop rather than returning the same message twice.
     * @throws IOException on read errors.
     */
    protected BaseMessageProvider(File fileOrDirectory, String encoding, boolean ordered, boolean noDuplicates)
            throws IOException {
        if (fileOrDirectory.isDirectory()) {
            File[] files = fileOrDirectory.listFiles();
            Arrays.sort(files);
            for (File f : files) {
                if (f.isFile()) {
                    readPayloadAndHeaders(f, Charset.forName(encoding));
                }
            }
        }
        else {
            readPayloadAndHeaders(fileOrDirectory, Charset.forName(encoding));
        }
        _ordered = ordered;
        _outlierPercentage = null;
        _outlierSize = 0;
        _noDuplicates = noDuplicates;
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
            _messageHeaderList.add(Collections.emptyMap());
            _messageDataList.add(createRandomMessageData(size));
            if (size < maxSize) {
                size = Math.min(size + step, maxSize);
            }
        }
        _ordered = false;
        _outlierPercentage = outlierPercentage;
        _outlierSize = outlierSize;
        _noDuplicates = false;
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
     * Create a JMS message for the specified session with a prepared payload and possibly prepared JMS
     * properties and/or checksum and length properties..
     *
     * @param session The session.
     * @param addIntegrityProperties The flag to add integrity properties.
     * @return message.
     * @throws JMSException on errors.
     */
    @Override
    public Message createMessageWithPayloadAndProperties(Session session, boolean addIntegrityProperties)
            throws JMSException {
        T messageData;
        Map<String, String> headers;
        if (_outlierPercentage != null && _random.nextDouble() < (_outlierPercentage.doubleValue() / 100.0)) {
            messageData = createRandomMessageData(_outlierSize);
            headers = Collections.emptyMap();
        }
        else {
            int index = getNextMessageDataIndex();
            if (index < 0) {
                // No duplicates and all messages returned once
                return null;
            }
            messageData = _messageDataList.get(index);
            headers = _messageHeaderList.get(index);
        }
        Message msg = createMessageWithPayload(session, messageData);
        for (Entry<String, String> entry : headers.entrySet()) {
            msg.setStringProperty(entry.getKey(), entry.getValue());
        }
        if (addIntegrityProperties) {
            msg.setStringProperty(CHECKSUM_PROPERTY_NAME, messageData.getChecksum());
            msg.setIntProperty(LENGTH_PROPERTY_NAME, messageData.getLength());
        }
        return msg;
    }

    private int getNextMessageDataIndex() {
        int numberOfMessages = _messageDataList.size();
        if (_ordered) {
            int index = _messageIndex.incrementAndGet();
            if (_noDuplicates && index > numberOfMessages) {
                return -1;
            }
            return index % numberOfMessages;

        }
        else {
            return _random.nextInt(numberOfMessages);
        }
    }

    private void readPayloadAndHeaders(File file, Charset encoding) throws IOException {
        String name = file.getName();
        if (name.endsWith(".payload")) {
            _messageDataList.add(createMessageData(Files.readAllBytes(file.toPath()), encoding));
            _messageHeaderList.add(readHeaders(
                new File(file.getParentFile(), name.substring(0, name.length() - ".payload".length()) + ".headers")));
        }
        else if (!name.endsWith(".headers")) {
            _messageDataList.add(createMessageData(Files.readAllBytes(file.toPath()), encoding));
            _messageHeaderList.add(Collections.emptyMap());
        }
    }

    private Map<String, String> readHeaders(File file) throws IOException {
        if (file.isFile()) {
            try (FileInputStream is = new FileInputStream(file)) {
                Properties props = new Properties();
                props.load(is);
                Map<String, String> headerMap = new TreeMap<>();
                for (Object key : props.keySet()) {
                    headerMap.put((String) key, props.getProperty((String) key));
                }
                return headerMap;
            }
        }
        else {
            return Collections.emptyMap();
        }
    }
}