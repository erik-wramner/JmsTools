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

public abstract class BaseStopController implements StopController {
    private final Object _monitor = new Object();

    @Override
    public boolean keepRunning() {
        if (shouldKeepRunning()) {
            return true;
        }
        releaseWaitingThreads();
        return false;
    }

    @Override
    public void waitForTimeoutOrDone(long timeToWaitMillis) {
        synchronized (_monitor) {
            try {
                if (keepRunning()) {
                    _monitor.wait(timeToWaitMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    protected abstract boolean shouldKeepRunning();

    private void releaseWaitingThreads() {
        synchronized (_monitor) {
            _monitor.notifyAll();
        }
    }
}
