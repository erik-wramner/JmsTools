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

import java.util.concurrent.TimeUnit;

/**
 * Stop controller that runs for a specified time.
 * 
 * @author Erik Wramner
 */
public class DurationStopController extends BaseStopController {
    private final long _endTimeMillis;

    public DurationStopController(int durationMinutes) {
        _endTimeMillis = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(durationMinutes, TimeUnit.MINUTES);
    }

    @Override
    protected boolean shouldKeepRunning() {
        return System.currentTimeMillis() < _endTimeMillis;
    }
}