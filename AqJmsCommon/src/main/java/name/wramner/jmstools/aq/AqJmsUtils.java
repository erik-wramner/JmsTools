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

import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;

import oracle.jms.AQjmsFactory;

public class AqJmsUtils {
    public static ConnectionFactory createConnectionFactory(String url, String user, String password)
                    throws JMSException {
        return AQjmsFactory.getConnectionFactory(url, createPropsWithUserAndPassword(user, password));
    }

    public static XAConnectionFactory createXAConnectionFactory(String url, String user, String password)
                    throws JMSException {
        return AQjmsFactory.getXAConnectionFactory(url, createPropsWithUserAndPassword(user, password));
    }

    private static Properties createPropsWithUserAndPassword(String user, String password) {
        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);
        return props;
    }
}
