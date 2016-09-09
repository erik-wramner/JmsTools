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

import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.util.ByteSequence;

/**
 * This object message adapter uses ActiveMQ-specific code in order to get and set
 * object message payload as raw bytes.
 * 
 * @author Erik Wramner
 */
public class AmqObjectMessageAdapter implements ObjectMessageAdapter {

    @Override
    public void setObjectPayload(ObjectMessage msg, byte[] rawPayload) throws JMSException {
        ((ActiveMQObjectMessage) msg).setContent(new ByteSequence(rawPayload));
    }

    @Override
    public byte[] getObjectPayload(ObjectMessage msg) throws JMSException {
        return ((ActiveMQObjectMessage) msg).getContent().getData();
    }
}
