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
package name.wramner.jmstools.producer;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * Delay message delivery for a given time. This is not part of JMS 1.1, so an adapter is needed in order to bridge the
 * differences between implementations. Even so many providers simply don't support scheduled messages.
 * 
 * @author Erik Wramner
 */
public interface DelayedDeliveryAdapter {

    /**
     * Delay delivery for the message the specified number of seconds if supported. JMS implementations are not required
     * to support scheduled messages in JMS 1.1, so if the provider lacks support the method can do nothing or throw.
     * 
     * @param msg The JMS message.
     * @param seconds The number of seconds to delay delivery.
     * @throws JMSException on errors.
     */
    void setDelayProperty(Message msg, int seconds) throws JMSException;
}
