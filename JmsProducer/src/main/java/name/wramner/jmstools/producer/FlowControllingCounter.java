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
package name.wramner.jmstools.producer;

import name.wramner.jmstools.counter.Counter;

public class FlowControllingCounter implements Counter {
    private final FlowController _flowController;
    private final Counter _counter;

    public FlowControllingCounter(Counter counter, FlowController flowController) {
        _counter = counter;
        _flowController = flowController;
    }

    @Override
    public void incrementCount(int count) {
        _counter.incrementCount(count);
        _flowController.sleepIfAboveLimit();
    }

    @Override
    public int getCount() {
        return _counter.getCount();
    }
}