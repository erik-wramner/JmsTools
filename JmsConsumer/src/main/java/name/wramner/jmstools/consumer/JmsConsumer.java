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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.jms.JMSException;

import com.atomikos.icatch.jta.UserTransactionManager;

import name.wramner.jmstools.JmsClient;
import name.wramner.jmstools.StatisticsLogger;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.rm.JmsResourceManagerFactory;
import name.wramner.jmstools.rm.ResourceManagerFactory;
import name.wramner.jmstools.rm.XAJmsResourceManagerFactory;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * A JMS consumer creates a configurable number of threads and dequeues messages from a given destination. It can be used for
 * benchmarking and correctness tests. Concrete subclasses provide support for specific JMS providers.
 *
 * @author Erik Wramner
 * @param <T> The concrete configuration class.
 */
public abstract class JmsConsumer<T extends JmsConsumerConfiguration> extends JmsClient<T> {

    private static final String LOG_FILE_BASE_NAME = "dequeued_messages_";

    @Override
    protected boolean isConfigurationValid(T config) {
        if(config.getReceiveTimeoutMillis() == 0 && config.getPollingDelayMillis() == 0) {
            System.out.println("Please specify a receive timeout or a polling delay!");
            return false;
        }
        return true;
    }

    @Override
    protected List<Thread> createThreadsWithWorkers(T config) throws JMSException {
        Counter messageCounter = config.createMessageCounter();
        Counter receiveTimeoutCounter = config.createReceiveTimeoutCounter();
        StopController stopController = config.createStopController(messageCounter, receiveTimeoutCounter);
        ResourceManagerFactory resourceManagerFactory = config.useXa()
                        ? new XAJmsResourceManagerFactory(new UserTransactionManager(),
                                        config.createXAConnectionFactory(), config.getDestinationName(),
                                        config.isDestinationTypeQueue())
                        : new JmsResourceManagerFactory(config.createConnectionFactory(), config.getDestinationName(),
                                        config.isDestinationTypeQueue(), !config.isNonTransactional());
        List<Thread> threads = createThreads(resourceManagerFactory, messageCounter, receiveTimeoutCounter,
            stopController, config);
        if (config.isStatisticsEnabled()) {
            threads.add(new Thread(new StatisticsLogger(stopController, messageCounter, receiveTimeoutCounter),
                "StatisticsLogger"));
        }
        return threads;
    }

    private List<Thread> createThreads(ResourceManagerFactory resourceManagerFactory, Counter messageCounter,
            Counter receiveTimeoutCounter, StopController stopController, T config) {
        String currentTimeString = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        File logDirectory = config.getLogDirectory();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.getThreads(); i++) {
            threads.add(new Thread(
                new DequeueWorker<T>(resourceManagerFactory, messageCounter, receiveTimeoutCounter, stopController,
                    logDirectory != null ? new File(logDirectory,
                        LOG_FILE_BASE_NAME + (i + 1) + "_" + currentTimeString + ".log") : null,
                    config),
                "DequeueWorker-" + (i + 1)));
        }
        return threads;
    }
}