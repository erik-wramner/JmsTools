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
package name.wramner.jmstools.aq;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import name.wramner.jmstools.messages.ObjectMessageAdapter;
import oracle.jms.AQjmsObjectMessage;

/**
 * Uses AQ-specific code to get and set object message payload as bytes.
 * 
 * @author Erik Wramner
 */
public class AqObjectMessageAdapter implements ObjectMessageAdapter {

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectPayload(ObjectMessage msg, byte[] rawPayload) throws JMSException {
        ((AQjmsObjectMessage) msg).setBytesData(rawPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getObjectPayload(ObjectMessage msg) throws JMSException {
        return ((AQjmsObjectMessage) msg).getBytesData();
    }
}
