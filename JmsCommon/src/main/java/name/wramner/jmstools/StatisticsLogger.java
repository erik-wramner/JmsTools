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
package name.wramner.jmstools;

import java.util.ArrayList;
import java.util.List;

import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.stopcontroller.StopController;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class logs delta values for counters every minute.
 * 
 * @author Erik Wramner
 */
public class StatisticsLogger implements Runnable {
    private static final long ONE_MINUTE_IN_MS = 60000L;
    private static final char SEPARATOR = '\t';
    private final Logger _statisticsLogger = LoggerFactory.getLogger("statistics");
    private final Logger _logger = LoggerFactory.getLogger(getClass());
    private final List<Counter> _counters = new ArrayList<>();
    private final StopController _stopController;

    /**
     * Constructor.
     * 
     * @param stopController The stop controller.
     * @param counters The counters to log statistics for.
     */
    public StatisticsLogger(StopController stopController, Counter... counters) {
        for (Counter counter : counters) {
            _counters.add(counter);
        }
        _stopController = stopController;
    }

    /**
     * Log delta values for counters every minute, then sleep and repeat until the stop controller returns false.
     */
    @Override
    public void run() {
        _logger.debug("Statistics logger started...");
        try {
            int[] prevCounts = new int[_counters.size()];
            StringBuilder sb = new StringBuilder(80);
            _stopController.waitForTimeoutOrDone(ONE_MINUTE_IN_MS);
            while (_stopController.keepRunning()) {
                for (int i = 0; i < prevCounts.length; i++) {
                    int count = _counters.get(i).getCount();
                    if (i > 0) {
                        sb.append(SEPARATOR);
                    }
                    sb.append(count - prevCounts[i]);
                    prevCounts[i] = count;
                }
                _statisticsLogger.info(sb.toString());

                sb.setLength(0);
                _stopController.waitForTimeoutOrDone(ONE_MINUTE_IN_MS);
            }
        } finally {
            _logger.debug("Statistics logger stopped.");
        }
    }
}
