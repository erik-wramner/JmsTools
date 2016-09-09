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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

/**
 * A message provider for {@link ObjectMessage} messages.
 * 
 * @author Erik Wramner
 */
public class ObjectMessageProvider extends BaseMessageProvider<BytesMessageData> {
    private final ObjectMessageAdapter _adapter;

    /**
     * Constructor for prepared messages.
     * 
     * @param directory The message directory.
     * @param adapter The object message adapter.
     * @throws IOException on failure to read files.
     */
    public ObjectMessageProvider(File directory, ObjectMessageAdapter adapter) throws IOException {
        super(directory, "UTF-8", true);
        _adapter = adapter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BytesMessageData createRandomMessageData(int size) {
        throw new UnsupportedOperationException("Random object messages not supported");
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
        ObjectMessage msg = session.createObjectMessage();
        _adapter.setObjectPayload(msg, messageData.getData());
        return msg;
    }
}