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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

/**
 * A message provider can create JMS messages enriched with a checksum property that can be checked on the other side.
 * <p>
 * Note that according to the JMS standard property names must follow the same rules as Java identifiers.
 * 
 * @author Erik Wramner
 */
public interface MessageProvider {
    static final String UNIQUE_MESSAGE_ID_PROPERTY_NAME = "EWJMSToolsUniqueMessageId";
    static final String CHECKSUM_PROPERTY_NAME = "EWJMSToolsPayloadChecksumMD5";
    static final String LENGTH_PROPERTY_NAME = "EWJMSToolsPayloadLength";

    /**
     * Create message with checksum and length properties.
     *
     * @param session The JMS session.
     * @return message.
     * @throws JMSException on JMS errors.
     */
    Message createMessageWithPayloadAndChecksumProperty(Session session) throws JMSException;
}