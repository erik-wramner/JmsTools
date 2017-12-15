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

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for stop controllers that handles the task of waking waiting threads up when the sub-class signals that it
 * is time to stop by returning false from {@link #shouldKeepRunning()}.
 *
 * @author Erik Wramner
 */
public abstract class BaseStopController implements StopController {
    protected final Logger _logger = LoggerFactory.getLogger(getClass());
    private final Object _monitor = new Object();
    private final AtomicBoolean _aborted = new AtomicBoolean(false);

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean keepRunning() {
        if (!_aborted.get() && shouldKeepRunning()) {
            return true;
        }
        _logger.debug("Stop controller done, releasing waiting threads");
        releaseWaitingThreads();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void waitForTimeoutOrDone(long timeToWaitMillis) {
        if (timeToWaitMillis > 0) {
            long endTime = System.currentTimeMillis() + timeToWaitMillis;
            synchronized (_monitor) {
                try {
                    while (timeToWaitMillis > 0L && keepRunning()) {
                        _monitor.wait(timeToWaitMillis);
                        timeToWaitMillis = endTime - System.currentTimeMillis();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort() {
        if (!_aborted.getAndSet(true)) {
            _logger.info("Stop controller aborted!");
            releaseWaitingThreads();
        }
    }

    /**
     * Check if done.
     *
     * @return true to keep running, false when done.
     */
    protected abstract boolean shouldKeepRunning();

    /**
     * Release any threads that are waiting for the stop controller.
     */
    private void releaseWaitingThreads() {
        synchronized (_monitor) {
            _monitor.notifyAll();
        }
    }
}
