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

import java.util.concurrent.atomic.AtomicReference;

import name.wramner.jmstools.counter.Counter;

/**
 * Stop controller that first delegates to another stop controller and then waits for a count. Though it can be used in
 * other scenarios it was designed for first running a test and then draining the queue before stopping the consumers.
 * 
 * @author Erik Wramner
 */
public class ParentAndThenCountStopController extends BaseStopController {
    private final StopController _parent;
    private final AtomicReference<StopController> _drainedControllerReference = new AtomicReference<>();
    private final Counter _receiveTimeoutCounter;
    private final int _count;

    /**
     * Constructor.
     * 
     * @param parent The parent stop controller that must complete first.
     * @param counter The counter.
     * @param count The count for the counter to reach after the parent has stopped.
     */
    public ParentAndThenCountStopController(StopController parent, Counter counter, int count) {
        _parent = parent;
        _receiveTimeoutCounter = counter;
        _count = count;
    }

    @Override
    public boolean shouldKeepRunning() {
        if (_parent.keepRunning()) {
            return true;
        }
        StopController drainedController = _drainedControllerReference.get();
        if (drainedController == null) {
            drainedController = new CountStopController(_receiveTimeoutCounter.getCount() + _count,
                            _receiveTimeoutCounter);
            _drainedControllerReference.set(drainedController);
        }
        return drainedController.keepRunning();
    }

}
