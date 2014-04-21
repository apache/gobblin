package com.linkedin.uif.scheduler;

import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ServiceManager;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.runtime.Metrics;

/**
 * A class that runs the {@link JobScheduler} in a daemon process for standalone deployment.
 */
public class SchedulerDaemon {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerDaemon.class);

    private final Properties properties;

    private final ServiceManager serviceManager;

    public SchedulerDaemon(Properties properties) throws Exception {
        this.properties = properties;
        JobScheduler jobScheduler = new JobScheduler(properties);
        this.serviceManager = new ServiceManager(Lists.newArrayList(
                // The order matters due to dependencies between services
                jobScheduler
        ));
    }

    /**
     * Start this scheduler daemon.
     */
    public void start() {
        if (Metrics.isEnabled(this.properties)) {
            long metricsReportInterval = Long.parseLong(this.properties.getProperty(
                    ConfigurationKeys.METRICS_REPORT_INTERVAL_KEY,
                    ConfigurationKeys.DEFAULT_METRICS_REPORT_INTERVAL));
            Metrics.startCsvReporter(metricsReportInterval,
                    this.properties.getProperty(ConfigurationKeys.METRICS_DIR_KEY));
        }

        // Add a shutdown hook so the task scheduler gets properly shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                // Give the services 5 seconds to stop to ensure that we are
                // responsive to shutdown requests
                LOG.info("Shutting down the scheduler daemon");
                try {
                    serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
                } catch (TimeoutException te) {
                    LOG.error("Timeout in stopping the service manager", te);
                }
            }

        });

        LOG.info("Starting the scheduler daemon");
        // Start the scheduler daemon
        this.serviceManager.startAsync();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: SchedulerDaemon <configuration properties file>");
            System.exit(1);
        }

        Properties properties = new Properties();
        // Load framework configuration properties
        properties.load(new FileReader(args[0]));
        // Start the scheduler daemon
        new SchedulerDaemon(properties).start();
    }
}
