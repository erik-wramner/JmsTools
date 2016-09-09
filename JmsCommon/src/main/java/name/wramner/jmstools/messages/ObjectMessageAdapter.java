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
import javax.jms.ObjectMessage;

/**
 * An object message adapter knows how to read and write the payload for object messages.
 * The default implementation uses serialization, but most JMS providers can manipulate
 * the raw payload as bytes. That is far more efficient and it also obviates the need
 * for having the payload's class in the class path.
 * 
 * @author Erik Wramner
 */
public interface ObjectMessageAdapter {

    /**
     * Set payload for object message based on raw bytes.
     * 
     * @param msg The object message.
     * @param rawPayload The payload in raw format.
     * @throws JMSException on errors.
     */
    void setObjectPayload(ObjectMessage msg, byte[] rawPayload) throws JMSException;

    /**
     * Get raw payload for object message.
     * 
     * @param msg The object message.
     * @return payload as raw bytes.
     * @throws JMSException on errors.
     */
    byte[] getObjectPayload(ObjectMessage msg) throws JMSException;
}
