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

import java.nio.charset.Charset;

import javax.jms.TextMessage;

/**
 * Message data for JMS {@link TextMessage} messages.
 */
public class TextMessageData extends ChecksummedMessageData {
    private static final Charset TEXT_ENCODING = Charset.forName("UTF-8");
    private final String _data;

    /**
     * Constructor.
     * <p>
     * Note that the checksum calculated by the super class uses the UTF-8 encoding.
     * 
     * @param data The message text.
     */
    public TextMessageData(String data) {
        super(data.getBytes(TEXT_ENCODING));
        _data = data;
    }

    /**
     * Get the message text.
     * 
     * @return text.
     */
    public String getData() {
        return _data;
    }
}