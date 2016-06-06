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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.jms.ConnectionFactory;

import name.wramner.jmstools.JmsClient;
import name.wramner.jmstools.StatisticsLogger;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * A JMS consumer creates a configurable number of threads and dequeues messages from a given queue. It can be used for
 * benchmarking and correctness tests. Concrete subclasses provide support for specific JMS providers.
 * 
 * @author Erik Wramner
 * @param <T> The concrete configuration class.
 */
public abstract class JmsConsumer<T extends JmsConsumerConfiguration> extends JmsClient<T> {

    private static final String LOG_FILE_BASE_NAME = "dequeued_messages_";

    @Override
    protected List<Thread> createThreadsWithWorkers(T config, ConnectionFactory connFactory) {
        Counter messageCounter = config.createMessageCounter();
        Counter receiveTimeoutCounter = config.createReceiveTimeoutCounter();
        StopController stopController = config.createStopController(messageCounter, receiveTimeoutCounter);
        List<Thread> threads = createThreads(connFactory, messageCounter, receiveTimeoutCounter, stopController, config);
        if (config.isStatisticsEnabled()) {
            threads.add(new Thread(new StatisticsLogger(stopController, messageCounter, receiveTimeoutCounter),
                            "StatisticsLogger"));
        }
        return threads;
    }

    private List<Thread> createThreads(ConnectionFactory connFactory, Counter messageCounter,
                    Counter receiveTimeoutCounter, StopController stopController, T config) {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.getThreads(); i++) {
            threads.add(new Thread(new DequeueWorker<T>(connFactory, messageCounter, receiveTimeoutCounter,
                            stopController, config.getLogDirectory() != null ? new File(config.getLogDirectory(),
                                            LOG_FILE_BASE_NAME + (i + 1) + ".log") : null, config), "DequeueWorker-"
                            + (i + 1)));
        }
        return threads;
    }
}