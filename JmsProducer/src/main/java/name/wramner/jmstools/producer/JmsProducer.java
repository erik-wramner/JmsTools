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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.jms.JMSException;

import com.atomikos.icatch.jta.UserTransactionManager;

import name.wramner.jmstools.JmsClient;
import name.wramner.jmstools.StatisticsLogger;
import name.wramner.jmstools.counter.Counter;
import name.wramner.jmstools.messages.MessageProvider;
import name.wramner.jmstools.rm.JmsResourceManagerFactory;
import name.wramner.jmstools.rm.ResourceManagerFactory;
import name.wramner.jmstools.rm.XAJmsResourceManagerFactory;
import name.wramner.jmstools.stopcontroller.StopController;

/**
 * A JMS producer creates a configurable number of threads and enqueues messages on a given destination. It can be used for
 * benchmarking and correctness tests. Concrete subclasses provide support for specific JMS providers.
 *
 * @author Erik Wramner
 * @param <T> The concrete configuration class.
 */
public abstract class JmsProducer<T extends JmsProducerConfiguration> extends JmsClient<T> {

    private static final String LOG_FILE_BASE_NAME = "enqueued_messages_";

    @Override
    protected List<Thread> createThreadsWithWorkers(T config) throws JMSException {
        MessageProvider messageProvider;
        try {
            messageProvider = config.createMessageProvider();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize message provider", e);
        }
        Counter counter = config.createMessageCounter();
        StopController stopController = config.createStopController(counter);
        ResourceManagerFactory resourceManagerFactory = config.useXa()
                ? new XAJmsResourceManagerFactory(new UserTransactionManager(), config.createXAConnectionFactory(),
                    config.getDestinationName(), config.isDestinationTypeQueue())
                : new JmsResourceManagerFactory(config.createConnectionFactory(), config.getDestinationName(),
                    config.isDestinationTypeQueue());
        List<Thread> threads = createThreads(resourceManagerFactory, counter, stopController, messageProvider, config);
        if (config.isStatisticsEnabled()) {
            threads.add(new Thread(new StatisticsLogger(stopController, counter), "StatisticsLogger"));
        }
        return threads;
    }

    private List<Thread> createThreads(ResourceManagerFactory resourceManagerFactory, Counter counter,
            StopController stopController, MessageProvider messageProvider, T config) {
        String currentTimeString = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < config.getThreads(); i++) {
            threads.add(new Thread(
                new EnqueueWorker<T>(resourceManagerFactory, counter, stopController, messageProvider,
                    config.getLogDirectory() != null ? new File(config.getLogDirectory(),
                        LOG_FILE_BASE_NAME + (i + 1) + "_" + currentTimeString + ".log") : null,
                    config),
                "EnqueueWorker-" + (i + 1)));
        }
        return threads;
    }
}