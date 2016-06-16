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

/**
 * A counter is thread-safe and can be incremented and read. A counter can keep track of the number of received messages
 * across all threads, for example. Given the intended use integer precision should be good enough; no one sends more
 * than {@link Integer#MAX_VALUE} messages in a performance test.
 * 
 * @author Erik Wramner
 */
public interface Counter {
    /**
     * Increment counter.
     * 
     * @param count The positive value to add to the counter.
     */
    void incrementCount(int count);

    /**
     * Get current value for counter.
     * 
     * @return current count.
     */
    int getCount();
}