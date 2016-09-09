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
package name.wramner.jmstools.consumer;

import name.wramner.jmstools.JmsClientConfiguration;
import name.wramner.jmstools.counter.AtomicCounter;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.stopcontroller.ChainStopController;
import name.wramner.jmstools.stopcontroller.CountStopController;
import name.wramner.jmstools.stopcontroller.DrainedStopController;
import name.wramner.jmstools.stopcontroller.DurationOrCountStopController;
import name.wramner.jmstools.stopcontroller.DurationStopController;
import name.wramner.jmstools.stopcontroller.RunForeverStopController;
import name.wramner.jmstools.stopcontroller.StopController;

import java.io.File;

import org.kohsuke.args4j.Option;

/**
 * JMS consumer configuration.
 * 
 * @author Erik Wramner
 */
public abstract class JmsConsumerConfiguration extends JmsClientConfiguration {
    private static final long DRAINED_TIMEOUT_MS = 1000L;

    @Option(name = "-drain", aliases = { "--until-drained" }, usage = "Run until all messages have been consumed")
    private boolean _untilDrained;

    @Option(name = "-verify", aliases = "--verify-checksum", usage = "Verify message checksums (somewhat expensive)")
    private boolean _verifyChecksum;

    @Option(name = "-timeout", aliases = "--receive-timeout-ms", usage = "Receive timeout in milliseconds, 0 means no wait")
    private int _receiveTimeoutMillis;

    @Option(name = "-delay", aliases = "--polling-delay-ms", usage = "Sleep time in milliseconds before next attempt"
            + " when no message is returned")
    private int _pollingDelayMillis;

    @Option(name = "-commitempty", aliases = "--commit-on-receive-timeout", usage = "Commit when a receive operation"
            + " has timed out without returning any data, may be necessary in order to keep"
            + " transaction timeouts in check")
    private boolean _shouldCommitOnReceiveTimeout = true;

    @Option(name = "-dir", aliases = "--message-file-directory", usage = "Save consumed messages to directory")
    private File _messageFileDirectory;

    public boolean shouldCommitOnReceiveTimeout() {
        return _shouldCommitOnReceiveTimeout;
    }

    public boolean shouldVerifyChecksum() {
        return _verifyChecksum;
    }

    public int getReceiveTimeoutMillis() {
        return _receiveTimeoutMillis;
    }

    public int getPollingDelayMillis() {
        return _pollingDelayMillis;
    }

    public Counter createReceiveTimeoutCounter() {
        return new AtomicCounter();
    }

    public StopController createStopController(Counter messageCounter, Counter receiveTimeoutCounter) {
        StopController stopController;

        if (_durationMinutes != null && _stopAfterMessages != null) {
            stopController = new DurationOrCountStopController(_stopAfterMessages.intValue(), messageCounter,
                _durationMinutes.intValue());
        }
        else if (_durationMinutes != null) {
            stopController = new DurationStopController(_durationMinutes.intValue());
        }
        else if (_stopAfterMessages != null) {
            stopController = new CountStopController(_stopAfterMessages.intValue(), messageCounter);
        }
        else if (_untilDrained) {
            return new DrainedStopController(messageCounter, receiveTimeoutCounter, DRAINED_TIMEOUT_MS);
        }
        else {
            return new RunForeverStopController();
        }

        if (_untilDrained) {
            stopController = new ChainStopController(stopController,
                new DrainedStopController(messageCounter, receiveTimeoutCounter, DRAINED_TIMEOUT_MS));
        }

        return stopController;
    }

    public File getMessageFileDirectory() {
        return _messageFileDirectory;
    }
}
