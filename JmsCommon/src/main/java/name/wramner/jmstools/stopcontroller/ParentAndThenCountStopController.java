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

public class ParentAndThenCountStopController implements StopController {
    private final StopController _parent;
    private final AtomicReference<StopController> _drainedControllerReference = new AtomicReference<>();
    private final Counter _receiveTimeoutCounter;

    public ParentAndThenCountStopController(StopController parent, Counter counter) {
        _parent = parent;
        _receiveTimeoutCounter = counter;
    }

    @Override
    public boolean keepRunning() {
        if (_parent.keepRunning()) {
            return true;
        }
        StopController drainedController = _drainedControllerReference.get();
        if (drainedController == null) {
            drainedController = new CountStopController(_receiveTimeoutCounter.getCount() + 1, _receiveTimeoutCounter);
            _drainedControllerReference.set(drainedController);
        }
        return drainedController.keepRunning();
    }

}
