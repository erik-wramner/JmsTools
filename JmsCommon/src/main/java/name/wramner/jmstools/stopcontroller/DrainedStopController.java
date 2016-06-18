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

import java.util.concurrent.atomic.AtomicLong;

import name.wramner.jmstools.counter.Counter;

/**
 * Stop controller that runs until the queue is drained. It turns out that this is easier said then done. It is quite
 * possible for not only one but several receive calls to return null even for a non-empty queue, in particular with
 * receiveNoWait or when a short timeout is used. In other words it is not enough to wait for that.
 * <p>
 * This implementation combines time with two counters: the number of received messages and the number of receive
 * timeouts. If, for a certain time, there are no received messages but receive timeouts, then the queue is considered
 * drained.
 * <p>
 * The disadvantage with this scheme (except that it can still fail, in particular for scheduled messages) is that it
 * will spend some time waiting when all messages have been consumed, so it makes it harder to measure the time needed
 * in order to consume x messages. That can still be done based on the logs, though.
 * 
 * @author Erik Wramner
 */
public class DrainedStopController extends BaseStopController {
    private final Counter _messageCounter;
    private final Counter _timeoutCounter;
    private final long _millisToWait;
    private final AtomicLong _nextCheckTimeMillis;
    private int _lastMessageCount;
    private int _lastTimeoutCount;
    private boolean _done;

    /**
     * Constructor.
     * 
     * @param messageCounter The message counter.
     * @param timeoutCounter The receive timeout counter.
     * @param millisToWait The time to wait for no messages, only timeouts, before considering the queue empty.
     */
    public DrainedStopController(Counter messageCounter, Counter timeoutCounter, long millisToWait) {
        _messageCounter = messageCounter;
        _timeoutCounter = timeoutCounter;
        _millisToWait = millisToWait;
        _nextCheckTimeMillis = new AtomicLong(System.currentTimeMillis() + millisToWait);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean shouldKeepRunning() {
        long now = System.currentTimeMillis();
        if (now < _nextCheckTimeMillis.get()) {
            return true;
        }
        synchronized (this) {
            if (_done) {
                return false;
            }
            if (now < _nextCheckTimeMillis.get()) {
                return true;
            }
            if (_lastMessageCount == _messageCounter.getCount() && _lastTimeoutCount < _timeoutCounter.getCount()) {
                _done = true;
                return false;
            }
            _lastMessageCount = _messageCounter.getCount();
            _lastTimeoutCount = _timeoutCounter.getCount();
            _nextCheckTimeMillis.set(now + _millisToWait);
        }
        return true;
    }
}