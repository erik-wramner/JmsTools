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
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * A message provider for {@link BytesMessage} messages.
 *
 * @author Erik Wramner
 */
public class BytesMessageProvider extends BaseMessageProvider<BytesMessageData> {
    /**
     * Constructor for prepared messages.
     *
     * @param directory The message directory.
     * @param encoding The encoding (not really used, passed to superclass).
     * @param commonHeaders The JMS headers common to all messages.
     * @param ordered The flag to send messages in alphabetical order or in random order.
     * @param noDuplicates The flag to stop rather than returning the same message twice.
     * @throws IOException on failure to read files.
     */
    public BytesMessageProvider(File directory, String encoding, Map<String, String> commonHeaders, boolean ordered,
                    boolean noDuplicates) throws IOException {
        super(directory, encoding, commonHeaders, ordered, noDuplicates);
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
    public BytesMessageProvider(int minSize, int maxSize, int numberOfMessages, Double outlierPercentage,
                    int outlierSize, Map<String, String> commonHeaders) {
        super(minSize, maxSize, numberOfMessages, outlierPercentage, outlierSize, commonHeaders);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BytesMessageData createRandomMessageData(int size) {
        byte[] data = new byte[size];
        _random.nextBytes(data);
        return new BytesMessageData(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BytesMessageData createMessageData(byte[] bytes, Charset characterEncoding) {
        return new BytesMessageData(bytes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Message createMessageWithPayload(Session session, BytesMessageData messageData) throws JMSException {
        BytesMessage msg = session.createBytesMessage();
        msg.writeBytes(messageData.getData());
        return msg;
    }
}