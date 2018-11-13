/*
 * Copyright 2018 Erik Wramner.
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
package name.wramner.jmstools.analyzer;

import java.sql.Timestamp;

/**
 * Flight time metrics for a period (minute).
 */
public class FlightTimeMetrics {
    private final Timestamp _period;
    private final int _count;
    private final int _min;
    private final int _max;
    private final int _median;
    private final int _percentile95;

    public FlightTimeMetrics(Timestamp period, int count, int min, int max, int median, int percentile95) {
        _period = period;
        _count = count;
        _min = min;
        _max = max;
        _median = median;
        _percentile95 = percentile95;
    }

    public Timestamp getPeriod() {
        return _period;
    }

    public int getCount() {
        return _count;
    }

    public int getMin() {
        return _min;
    }

    public int getMax() {
        return _max;
    }

    public int getMedian() {
        return _median;
    }

    public int getPercentile95() {
        return _percentile95;
    }
}