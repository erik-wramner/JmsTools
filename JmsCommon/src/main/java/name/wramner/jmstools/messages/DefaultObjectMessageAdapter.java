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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

/**
 * Object message adapter that uses standard serialization.
 * 
 * @author Erik Wramner
 */
public class DefaultObjectMessageAdapter implements ObjectMessageAdapter {

    /**
     * Set payload for object message based on raw bytes.
     * 
     * @param msg The object message.
     * @param rawPayload The payload in raw format.
     * @throws JMSException on errors.
     */
    @Override
    public void setObjectPayload(ObjectMessage msg, byte[] rawPayload) throws JMSException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(rawPayload);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            msg.setObject((Serializable) ois.readObject());
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        } catch (ClassNotFoundException e) {
            throw new JMSException(e.getMessage());
        }
    }

    /**
     * Get raw payload for object message.
     * 
     * @param msg The object message.
     * @return payload as raw bytes.
     * @throws JMSException on errors.
     */
    @Override
    public byte[] getObjectPayload(ObjectMessage msg) throws JMSException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(msg.getObject());
            oos.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new JMSException(e.getMessage());
        }
    }
}
