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
package name.wramner.jmstools.stopcontroller;

/**
 * A stop controller knows when the test is done and producers/consumers should stop.
 *
 * @author Erik Wramner
 */
public interface StopController {
    /**
     * Keep running or stop.
     *
     * @return true to keep running.
     */
    boolean keepRunning();

    /**
     * Wait for the specified time to elapse or return early when done.
     *
     * @param timeToWaitMillis The time to wait in milliseconds.
     */
    void waitForTimeoutOrDone(long timeToWaitMillis);

    /**
     * Make the controller abort, for example after an error.
     */
    void abort();
}