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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.atomikos.icatch.config.UserTransactionService;
import com.atomikos.icatch.config.UserTransactionServiceImp;

/**
 * Base class for JMS producers and consumers with support for command line parsing and thread creation/joining. It also
 * initializes and stops the transaction manager for XA transactions if they are enabled.
 * 
 * @author Erik Wramner
 * @param <T> configuration class.
 */
public abstract class JmsClient<T extends JmsClientConfiguration> {
    private UserTransactionService _userTransactionService;

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
                if (config.useXa()) {
                    _userTransactionService = new UserTransactionServiceImp();
                    _userTransactionService.init(createAtomikosInitializationProperties(config));
                }
                List<Thread> threads = createThreadsWithWorkers(config);
                startThreads(threads);
                waitForThreadsToComplete(threads);
            } catch (Exception e) {
                System.out.println("Failed with exception: " + e.getMessage());
                e.printStackTrace(System.out);
            } finally {
                if (_userTransactionService != null) {
                    int maxWaitSeconds = config.getJtaTimeoutSeconds() + 10;
                    _userTransactionService.shutdown(TimeUnit.MILLISECONDS.convert(maxWaitSeconds, TimeUnit.SECONDS));
                }
            }
            System.exit(0);
        }
    }

    /**
     * Create initialization properties for the Atomikos transaction manager.
     *
     * @param config The configuration.
     * @return initialization properties.
     * @throws UnknownHostException on failure to resolve local host.
     */
    protected Properties createAtomikosInitializationProperties(T config) throws UnknownHostException {
        Properties props = new Properties();
        if (config.isTmLogDisabled()) {
            props.setProperty("com.atomikos.icatch.enable_logging", "false");
        } else {
            props.setProperty("com.atomikos.icatch.checkpoint_interval", String.valueOf(TimeUnit.MILLISECONDS.convert(
                    config.getCheckpointIntervalSeconds(), TimeUnit.SECONDS)));
            props.setProperty("com.atomikos.icatch.recovery_delay", String.valueOf(TimeUnit.MILLISECONDS.convert(
                    config.getRecoveryIntervalSeconds(), TimeUnit.SECONDS)));
            if (config.getXaLogBaseDir() != null) {
                props.setProperty("com.atomikos.icatch.log_base_dir", config.getXaLogBaseDir().getAbsolutePath());
            }
        }
        props.setProperty("com.atomikos.icatch.automatic_resource_registration", "true");
        props.setProperty("com.atomikos.icatch.max_actives", String.valueOf(config.getThreads() + 1));
        String jtaTimeoutMillis = String.valueOf(TimeUnit.MILLISECONDS.convert(config.getJtaTimeoutSeconds(),
                TimeUnit.SECONDS));
        props.setProperty("com.atomikos.icatch.max_timeout", jtaTimeoutMillis);
        props.setProperty("com.atomikos.icatch.default_jta_timeout", jtaTimeoutMillis);
        props.setProperty("com.atomikos.icatch.tm_unique_name", config.getTmName() != null ? config.getTmName()
                : createTmName());
        return props;
    }

    /**
     * Build a reasonably unique transaction manager name.
     *
     * @return name.
     * @throws UnknownHostException on failure to find IP address for local host.
     */
    protected String createTmName() throws UnknownHostException {
        return getClass().getSimpleName() + "-" + InetAddress.getLocalHost().getHostAddress();
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