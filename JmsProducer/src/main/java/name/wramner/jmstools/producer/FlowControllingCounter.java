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

/**
 * This is a counter that doubles as flow controller. When the count is incremented it invokes the flow controller. If
 * it determines that the producer is too fast it sleeps for a while.
 * 
 * @author Erik Wramner
 */
public class FlowControllingCounter implements Counter {
    private final FlowController _flowController;
    private final Counter _counter;

    /**
     * Constructor.
     * 
     * @param counter The message counter.
     * @param flowController The flow controller.
     */
    public FlowControllingCounter(Counter counter, FlowController flowController) {
        _counter = counter;
        _flowController = flowController;
    }

    /**
     * Increment count, sleep if going to fast.
     * 
     * @param count The count to add to the counter.
     */
    @Override
    public void incrementCount(int count) {
        _counter.incrementCount(count);
        _flowController.sleepIfAboveLimit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getCount() {
        return _counter.getCount();
    }
}