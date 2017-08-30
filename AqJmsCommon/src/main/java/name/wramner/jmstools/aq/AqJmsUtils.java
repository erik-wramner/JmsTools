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
import java.util.Properties;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnectionFactory;

/**
 * Utility methods for AQ.
 * <p>
 * The Oracle libraries are not publicly available using Maven. In order to keep the build
 * without dependencies they are used indirectly with reflection.
 *
 * @author Erik Wramner
 */
public class AqJmsUtils {

    /**
     * Create a JMS connection factory for normal (non-XA) connections.
     *
     * @param url The Oracle JDBC URL.
     * @param user The database user.
     * @param password The database password.
     * @return connection factory.
     * @throws JMSException on JMS errors.
     * @throws IllegalStateException if the Oracle AQAPI is missing.
     */
    public static ConnectionFactory createConnectionFactory(String url, String user, String password)
            throws JMSException {
        Method m = resolveAqJmsFactoryMethod("getConnectionFactory");
        try {
            return (ConnectionFactory) m.invoke(null, url, createPropsWithUserAndPassword(user, password));
        }
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new JMSException("Failed to open AQ JMS connection factory");
        }
    }

    /**
     * Create a JMS connection factory for XA connections.
     *
     * @param url The Oracle JDBC URL.
     * @param user The database user.
     * @param password The database password.
     * @return XA connection factory.
     * @throws JMSException on JMS errors.
     * @throws IllegalStateException if the Oracle AQAPI is missing.
     */
    public static XAConnectionFactory createXAConnectionFactory(String url, String user, String password)
            throws JMSException {
        Method m = resolveAqJmsFactoryMethod("getXAConnectionFactory");
        try {
            return (XAConnectionFactory) m.invoke(null, url, createPropsWithUserAndPassword(user, password));
        }
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new JMSException("Failed to open AQ JMS connection factory");
        }
    }

    private static Method resolveAqJmsFactoryMethod(String methodName) {
        try {
            Class<?> aqJmsFactoryClass = Class.forName("oracle.jms.AQjmsFactory");
            return aqJmsFactoryClass.getMethod(methodName, String.class, Properties.class);
        }
        catch (ClassNotFoundException e) {
            throw new IllegalStateException("Oracle AQ classes from aqapi not found in class path!", e);
        }
        catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                "Failed to find method oracle.jms.AQjmsFactory#" + methodName + "(String, Properties)", e);
        }
        catch (SecurityException e) {
            throw new IllegalStateException(
                "Not allowed to call oracle.jms.AQjmsFactory#" + methodName + "(String, Properties)", e);
        }
    }

    private static Properties createPropsWithUserAndPassword(String user, String password) {
        Properties props = new Properties();
        props.put("user", user);
        props.put("password", password);
        return props;
    }
}
