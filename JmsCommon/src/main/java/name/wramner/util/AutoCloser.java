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
package name.wramner.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This utility class is a stop-gap solution as many important resources do not implement {@link AutoCloseable}. Simply
 * create an instance of this class in a try statement and register the resources later; then forget about the cleanup.
 * 
 * @author Erik Wramner
 */
public class AutoCloser implements AutoCloseable {
    private final List<Object> _resources = new ArrayList<Object>();

    /**
     * Default constructor.
     */
    public AutoCloser() {
    }

    /**
     * Constructor with resource to close.
     * 
     * @param o The resource.
     */
    public AutoCloser(Object o) {
        _resources.add(o);
    }

    /**
     * Add resource to close.
     * 
     * @param o The resource.
     * @return this.
     */
    public AutoCloser add(Object o) {
        synchronized (_resources) {
            _resources.add(o);
        }
        return this;
    }

    /**
     * Close all registered resources in reverse order.
     * 
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() {
        synchronized (_resources) {
            Collections.reverse(_resources);
            _resources.stream().forEach(e -> closeSafely(e));
        }
    }

    /**
     * Close the object and ignore any exceptions.
     * 
     * @param o The object.
     * @throws IllegalStateException if there is no close method.
     */
    private void closeSafely(Object o) {
        if (o != null) {
            if (o instanceof Closeable) {
                try {
                    ((Closeable) o).close();
                } catch (IOException e) {
                    // Ignore exception
                }
            } else {
                Method closeMethod;
                try {
                    closeMethod = o.getClass().getMethod("close");
                } catch (NoSuchMethodException e) {
                    throw new IllegalStateException("AutoCloser can't work with " + o.getClass().getName()
                                    + ": no close method!", e);
                }
                try {
                    closeMethod.invoke(o);
                } catch (Exception e) {
                    // Ignore all exceptions
                }
            }
        }
    }
}
