package com.linkedin.uif.scheduler;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is the main class that each UIF worker node runs.
 */
public class Worker {

    private static final Log LOG = LogFactory.getLog(Worker.class);

    // We use this to manage all services running within the worker
    private final ServiceManager serviceManager;

    public Worker(Properties properties) {
        TaskManager taskManager = new TaskManager(properties);
        WorkUnitManager workUnitManager = new WorkUnitManager(properties, taskManager);
        this.serviceManager = new ServiceManager(
                Lists.newArrayList(workUnitManager, taskManager));
    }

    /**
     * Start this worker.
     */
    public void start() {
        this.serviceManager.addListener(new ServiceManager.Listener() {

            @Override
            public void stopped() {
                LOG.info("Wroker has been stopped");
            }

            @Override
            public void healthy() {
                LOG.info("All services are health and running");
                // Report services' uptimes
                for (Map.Entry<Service, Long> entry : serviceManager.startupTimes().entrySet()) {
                    LOG.info(String.format("Service %s is healthy with an uptime of %dms",
                            entry.getKey().toString(), entry.getValue()));
                }
            }

            @Override
            public void failure(Service service) {
                LOG.error(String.format("Service %s failed for the following reason:\n\t%s",
                        service.toString(), service.failureCause().toString()));
                System.exit(1);
            }

        }, Executors.newSingleThreadExecutor());

        // Add a shutdown hook so the task scheduler gets properly shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                // Give the services 5 seconds to stop to ensure that we are
                // responsive to shutdown requests
                LOG.info("Shutting down the worker");
                try {
                    serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
                } catch (TimeoutException te) {
                    LOG.error(te);
                }
            }

        });

        LOG.info("Starting the worker with configured services");
        // Start the worker
        this.serviceManager.startAsync();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: Worker <work configuration properties file>");
            System.exit(1);
        }

        Properties properties = new Properties();
        // Load worker configuration properties
        properties.load(new FileReader(args[0]));
        // Start the worker
        new Worker(properties).start();
    }
}
