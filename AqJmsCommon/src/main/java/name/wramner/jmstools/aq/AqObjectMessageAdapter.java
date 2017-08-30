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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import name.wramner.jmstools.messages.ObjectMessageAdapter;

/**
 * Uses AQ-specific code to get and set object message payload as bytes.
 * <p>
 * The Oracle libraries are not publicly available using Maven. In order to keep the build
 * without dependencies they are used indirectly with reflection.
 *
 * @author Erik Wramner
 */
public class AqObjectMessageAdapter implements ObjectMessageAdapter {
    private final Method _getMethod;
    private final Method _setMethod;

    public AqObjectMessageAdapter() {
        try {
            Class<?> aqObjectMessageClass = Class.forName("oracle.jms.AQjmsObjectMessage");
            _setMethod = aqObjectMessageClass.getMethod("setBytesData", byte[].class);
            _getMethod = aqObjectMessageClass.getMethod("getBytesData");
        }
        catch (Exception e) {
            throw new IllegalStateException(
                "Unable to find AQjmsObjectMessage methods, verify that aqapi is in the class path", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setObjectPayload(ObjectMessage msg, byte[] rawPayload) throws JMSException {
        try {
            _setMethod.invoke(msg, rawPayload);
        }
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new JMSException("Failed to call AQjmsObjectMessage#setBytesData(byte[]) with reflection");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getObjectPayload(ObjectMessage msg) throws JMSException {
        try {
            return (byte[]) _getMethod.invoke(msg);
        }
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new JMSException("Failed to call AQjmsObjectMessage#getBytesData() with reflection");
        }
    }
}
