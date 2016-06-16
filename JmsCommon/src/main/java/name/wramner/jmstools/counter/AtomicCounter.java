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
package name.wramner.jmstools.counter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Counter} implemented with atomics.
 * 
 * @author Erik Wramner
 */
public class AtomicCounter implements Counter {
    private final AtomicInteger _count = new AtomicInteger();

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementCount(int count) {
        _count.addAndGet(count);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getCount() {
        return _count.get();
    }
}