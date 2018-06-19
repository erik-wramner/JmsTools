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
import java.util.Collections;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * A message provider for {@link TextMessage} messages.
 *
 * @author Erik Wramner
 */
public class TextMessageProvider extends BaseMessageProvider<TextMessageData> {

    /**
     * Constructor for single prepared message.
     * 
     * @param data The data.
     * @param headers The headers.
     * @param noDuplicates The flag to stop instead of returning the same message twice.
     */
    public TextMessageProvider(String data, Map<String, String> headers, boolean noDuplicates) {
        super(new TextMessageData(data), Collections.emptyMap(), noDuplicates);
    }

    /**
     * Constructor for prepared messages.
     *
     * @param directory The message directory.
     * @param encoding The encoding for the files.
     * @param commonHeaders The JMS headers common to all messages.
     * @param ordered The flag to send messages in alphabetical order or in random order.
     * @param noDuplicates The flag to stop rather than returning the same message twice.
     * @throws IOException on failure to read files.
     */
    public TextMessageProvider(File directory, String encoding, Map<String, String> commonHeaders, boolean ordered,
                    boolean noDuplicates) throws IOException {
        super(directory, encoding, null, ordered, noDuplicates);
    }

    /**
     * Constructor for random messages.
     *
     * @param minSize The minimum size in bytes.
     * @param maxSize The maximum size in bytes.
     * @param numberOfMessages The number of messages to prepare.
     * @param outlierPercentage The percentage of messages outside the normal range.
     * @param outlierSize The size of the messages outside the range, typically very large.
     * @param commonHeaders The JMS headers common to all messages.
     */
    public TextMessageProvider(int minSize, int maxSize, int numberOfMessages, Double outlierPercentage,
                    int outlierSize, Map<String, String> commonHeaders) {
        super(minSize, maxSize, numberOfMessages, outlierPercentage, outlierSize, commonHeaders);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TextMessageData createRandomMessageData(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            // Add a random printable ASCII character
            sb.append((char) (0x20 + _random.nextInt(0x7e - 0x20)));
        }
        return new TextMessageData(sb.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TextMessageData createMessageData(byte[] bytes, Charset characterEncoding) {
        return new TextMessageData(new String(bytes, characterEncoding));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Message createMessageWithPayload(Session session, TextMessageData messageData) throws JMSException {
        return session.createTextMessage(messageData.getData());
    }
}