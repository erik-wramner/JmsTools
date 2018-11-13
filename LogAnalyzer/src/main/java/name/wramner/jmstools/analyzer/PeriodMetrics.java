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
 * Metrics for a period, normally a minute or second.
 */
public class PeriodMetrics {
    private final Timestamp periodStart;
    private final int _producedCount;
    private final int _consumedCount;
    private final long _producedBytes;
    private final long _consumedBytes;
    private final int _maxProducedSize;
    private final int _maxConsumedSize;
    private final int _medianProducedSize;
    private final int _medianConsumedSize;

    /**
     * Constructor.
     *
     * @param periodStart The period start time.
     * @param producedCount The number of produced messages in the period.
     * @param consumedCount The number of consumed messages in the period.
     * @param producedBytes The total number of bytes for messages produced in the period.
     * @param consumedBytes The total number of bytes for messages consumed in the period.
     * @param maxProducedSize The largest message size produced in the period.
     * @param maxConsumedSize The largest message size consumed in the period.
     * @param medianProducedSize The median (50th percentile) produced message size.
     * @param medianConsumedSize The median (50th percentile) consumed message size.
     */
    public PeriodMetrics(Timestamp periodStart, int producedCount, int consumedCount, long producedBytes,
                    long consumedBytes, int maxProducedSize, int maxConsumedSize, int medianProducedSize,
                    int medianConsumedSize) {
        this.periodStart = periodStart;
        _producedCount = producedCount;
        _consumedCount = consumedCount;
        _producedBytes = producedBytes;
        _consumedBytes = consumedBytes;
        _maxProducedSize = maxProducedSize;
        _maxConsumedSize = maxConsumedSize;
        _medianProducedSize = medianProducedSize;
        _medianConsumedSize = medianConsumedSize;
    }

    public Timestamp getPeriodStart() {
        return periodStart;
    }

    public int getTotal() {
        return getProduced() + getConsumed();
    }

    public int getProduced() {
        return _producedCount;
    }

    public int getConsumed() {
        return _consumedCount;
    }

    public long getTotalBytes() {
        return getProducedBytes() + getConsumedBytes();
    }

    public long getProducedBytes() {
        return _producedBytes;
    }

    public long getConsumedBytes() {
        return _consumedBytes;
    }

    public double getAverageProducedSize() {
        return _producedCount > 0 ? ((double) _producedBytes) / _producedCount : 0;
    }

    public double getAverageConsumedSize() {
        return _consumedCount > 0 ? ((double) _consumedBytes) / _consumedCount : 0;
    }

    public int getMaxProducedSize() {
        return _maxProducedSize;
    }

    public int getMaxConsumedSize() {
        return _maxConsumedSize;
    }

    public int getMedianProducedSize() {
        return _medianProducedSize;
    }

    public int getMedianConsumedSize() {
        return _medianConsumedSize;
    }
}