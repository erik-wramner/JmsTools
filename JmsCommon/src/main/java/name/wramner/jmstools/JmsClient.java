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

import java.util.List;

import javax.jms.JMSException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 * Base class for JMS producers and consumers with support for command line parsing and thread creation/joining.
 * 
 * @author Erik Wramner
 * @param <T> configuration class.
 */
public abstract class JmsClient<T extends JmsClientConfiguration> {

    /**
     * Parse command line arguments and initialize configuration, create threads with workers, start the threads and
     * wait for completion. Exit on errors.
     * 
     * @param args The command line.
     */
    public void run(String[] args) {
        T config = createConfiguration();
        if (parseCommandLine(args, config)) {
            try {
                List<Thread> threads = createThreadsWithWorkers(config);
                startThreads(threads);
                waitForThreadsToComplete(threads);
            } catch (Exception e) {
                System.out.println("Failed with exception: " + e.getMessage());
                e.printStackTrace(System.out);
                System.exit(1);
            }
        }
    }

    /**
     * Create configuration. Sub-classes should create provider-specific configuration classes.
     * 
     * @return configuration.
     */
    protected abstract T createConfiguration();

    /**
     * Create threads with workers.
     * 
     * @param config The initialized configuration.
     * @return list with threads, not started.
     * @throws JMSException on JMS errors.
     */
    protected abstract List<Thread> createThreadsWithWorkers(T config) throws JMSException;

    /**
     * Parse the command line into the specified configuration.
     * 
     * @param args The command line.
     * @param config The configuration class.
     * @return true if successful, false on errors such as missing arguments.
     */
    protected boolean parseCommandLine(String[] args, T config) {
        CmdLineParser parser = new CmdLineParser(config);
        try {
            parser.parseArgument(args);
            return true;
        } catch (CmdLineException e) {
            printUsage(parser);
            System.out.println("Error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Wait for all threads to complete, exit if interrupted.
     * 
     * @param threads The list with threads.
     */
    protected void waitForThreadsToComplete(List<Thread> threads) {
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                System.err.println("*** Interrupted - killing remaining threads!");
                System.exit(0);
            }
        }
    }

    /**
     * Start all threads.
     * 
     * @param threads The list with threads to start.
     */
    protected void startThreads(List<Thread> threads) {
        threads.forEach(t -> t.start());
    }

    /**
     * Print usage. All supported options are listed.
     * 
     * @param parser The parser.
     */
    protected void printUsage(CmdLineParser parser) {
        System.out.println("Usage: java " + getClass().getName() + " [options]");
        System.out.println();
        System.out.println("Where the options are:");
        parser.printUsage(System.out);
        System.out.println();
    }
}