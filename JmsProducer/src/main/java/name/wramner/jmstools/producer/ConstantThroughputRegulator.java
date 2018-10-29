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

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * This class checks the overall throughput every minute and adjusts the sleep time in order to increase or decrease the
 * number of messages sent.
 *
 * @author Erik Wramner
 */
public class ConstantThroughputRegulator implements Runnable {
    private static final int ONE_MINUTE_IN_MS = 60_000;
    private final Logger _logger = LoggerFactory.getLogger(getClass());
    private final Counter _counter;
    private final StopController _stopController;
    private final int _messagesPerMinute;
    private final AtomicInteger _sleepTimeMillis;
    private final int _threads;
    private final int _batchSize;
    private final int _maxDampingFactor;
    private final int _dampingIterationFactor;

    /**
     * Constructor.
     *
     * @param stopController The stop controller.
     * @param tpm The target TPM (messages per minute).
     * @param sleepTimeMillis The sleep time per request in milliseconds.
     * @param counter The counter for messages.
     * @param threads The number of threads.
     * @param batchSize The number of messages per commit (and sleep).
     */
    public ConstantThroughputRegulator(StopController stopController, int tpm, AtomicInteger sleepTimeMillis,
                    Counter counter, int threads, int batchSize) {
        this(stopController, tpm, sleepTimeMillis, counter, threads, batchSize, 5, 2);
    }

    /**
     * Constructor.
     *
     * @param stopController The stop controller.
     * @param tpm The target TPM (messages per minute).
     * @param sleepTimeMillis The sleep time per request in milliseconds.
     * @param counter The message counter.
     * @param threads The number of threads.
     * @param batchSize The number of messages per commit (and sleep).
     * @param maxDampingFactor The maximum damping factor.
     * @param dampingIterationFactor The parameter to increase damping over time.
     */
    public ConstantThroughputRegulator(StopController stopController, int tpm, AtomicInteger sleepTimeMillis,
                    Counter counter, int threads, int batchSize, int maxDampingFactor, int dampingIterationFactor) {
        _stopController = stopController;
        _counter = counter;
        _sleepTimeMillis = sleepTimeMillis;
        _messagesPerMinute = tpm;
        _threads = threads;
        _batchSize = batchSize;
        _maxDampingFactor = maxDampingFactor;
        _dampingIterationFactor = dampingIterationFactor;
    }

    /**
     * Compute TPM for the most recent minute, then adjust the sleep time accordingly.
     */
    @Override
    public void run() {
        _logger.debug("Constant throughput regulator started, goal is {} messages per minute", _messagesPerMinute);
        try {
            int prevCount = 0;
            int tpm = 0;
            _stopController.waitForTimeoutOrDone(ONE_MINUTE_IN_MS);

            for (int iterations = 0; _stopController.keepRunning(); iterations++) {
                int count = _counter.getCount();
                tpm = count - prevCount;
                prevCount = count;

                if (tpm != _messagesPerMinute) {
                    int currentSleepTime = _sleepTimeMillis.get();
                    int averageProcessingTime = calculateAverageProcessingTimeMillis(tpm, ONE_MINUTE_IN_MS, _threads,
                                    _batchSize, currentSleepTime);
                    int idealSleepTime = Math
                                    .max((_threads * ONE_MINUTE_IN_MS - (_messagesPerMinute * averageProcessingTime))
                                                    / _messagesPerMinute, 0)
                                    * _batchSize;
                    int newSleepTime = computeSleepTimeWithDamping(currentSleepTime, idealSleepTime, iterations);
                    if (newSleepTime != currentSleepTime) {
                        _sleepTimeMillis.set(newSleepTime);
                        _logger.info("Adjusted sleep time from {} to {} ms (ideal {} ms, target {} tpm, actual {} tpm, average processing time {} ms)",
                                        currentSleepTime, newSleepTime, idealSleepTime, _messagesPerMinute, tpm,
                                        averageProcessingTime);
                    }
                }
                _stopController.waitForTimeoutOrDone(ONE_MINUTE_IN_MS);
            }
        } finally {
            _logger.debug("Constant throughput regulator stopped");
        }
    }

    /**
     * Compute the next sleep time based on the current, the ideal for the most recent period and the number of
     * iterations. Damping should increase over time, preventing drastic changes in sleep time.
     *
     * @param currentSleepTime The current sleep time in milliseconds.
     * @param idealSleepTime The sleep time that would have been ideal for the last period.
     * @param iterations The number of periods observed so far.
     * @return sleep time for next period.
     */
    private int computeSleepTimeWithDamping(int currentSleepTime, int idealSleepTime, int iterations) {
        if (idealSleepTime != currentSleepTime && _maxDampingFactor > 1 && iterations > _dampingIterationFactor) {
            int dampingFactor = Math.min(1 + iterations / _dampingIterationFactor, _maxDampingFactor);
            int delta = idealSleepTime - currentSleepTime;
            int adjustedDelta = Math.max(Math.abs(delta) / dampingFactor, 1);
            _logger.debug("Damping factor {} delta {} adjusted {}", dampingFactor, delta, adjustedDelta);
            return delta < 0 ? currentSleepTime - adjustedDelta : currentSleepTime + adjustedDelta;
        } else {
            return idealSleepTime;
        }
    }

    /**
     * Compute average processing time per request (not transaction) for the last period.
     *
     * @param numberOfRequests The number of requests performed in the period.
     * @param periodTimeMillis The period length in milliseconds.
     * @param threads The number of worker threads.
     * @param batchSize The batch size (messages per commit and hence sleep).
     * @param sleepTimePerBatchMillis The sleep time per batch/commit.
     * @return average processing time for the completed requests.
     */
    private int calculateAverageProcessingTimeMillis(int numberOfRequests, int periodTimeMillis, int threads,
                    int batchSize, int sleepTimePerBatchMillis) {
        if (numberOfRequests == 0) {
            return periodTimeMillis;
        }
        double requestsPerThread = (double) numberOfRequests / threads;
        double batchesPerThread = requestsPerThread / batchSize;
        double totalProcessingTimePerThread = periodTimeMillis - (batchesPerThread * sleepTimePerBatchMillis);
        double averageProcessingTime = totalProcessingTimePerThread / requestsPerThread;
        return (int) Math.ceil(averageProcessingTime);
    }
}
