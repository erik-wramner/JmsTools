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

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

public class BytesMessageProvider extends BaseMessageProvider<BytesMessageData> {
    public BytesMessageProvider(File directory, String encoding, boolean ordered) throws IOException {
        super(directory, encoding, ordered);
    }

    public BytesMessageProvider(int minSize, int maxSize, int numberOfMessages, Double outlierPercentage,
                    int outlierSize) {
        super(minSize, maxSize, numberOfMessages, outlierPercentage, outlierSize);
    }

    @Override
    protected BytesMessageData createRandomMessageData(int size) {
        byte[] data = new byte[size];
        _random.nextBytes(data);
        return new BytesMessageData(data);
    }

    @Override
    protected BytesMessageData createMessageData(byte[] bytes, Charset characterEncoding) {
        return new BytesMessageData(bytes);
    }

    @Override
    protected Message createMessageWithPayload(Session session, BytesMessageData messageData) throws JMSException {
        BytesMessage msg = session.createBytesMessage();
        msg.writeBytes(messageData.getData());
        return msg;
    }
}