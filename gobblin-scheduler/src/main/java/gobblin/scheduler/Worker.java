/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.scheduler;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;
import gobblin.runtime.WorkUnitManager;
import gobblin.runtime.local.LocalJobManager;
import gobblin.runtime.local.LocalTaskStateTracker;
import gobblin.util.ExecutorsUtils;


/**
 * This is the main class that each Gobblin worker node runs.
 *
 * @author ynli
 */
@Deprecated
public class Worker {

  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  // We use this to manage all services running within the worker
  private final ServiceManager serviceManager;

  public Worker(Properties properties)
      throws Exception {
    // The worker runs the following services
    TaskExecutor taskExecutor = new TaskExecutor(properties);
    TaskStateTracker taskStateTracker = new LocalTaskStateTracker(properties, taskExecutor);
    WorkUnitManager workUnitManager = new WorkUnitManager(taskExecutor, taskStateTracker);
    LocalJobManager jobManager = new LocalJobManager(workUnitManager, properties);
    ((LocalTaskStateTracker) taskStateTracker).setJobManager(jobManager);

    this.serviceManager = new ServiceManager(Lists.newArrayList(
        // The order matters due to dependencies between services
        taskExecutor, taskStateTracker, workUnitManager, jobManager));
  }

  /**
   * Start this worker.
   */
  public void start() {
    this.serviceManager.addListener(new ServiceManager.Listener() {

      @Override
      public void stopped() {
        LOG.info("Worker has been stopped");
      }

      @Override
      public void healthy() {
        LOG.info("All services are health and running");
        // Report services' uptimes
        Map<Service, Long> startupTimes = serviceManager.startupTimes();
        for (Map.Entry<Service, Long> entry : startupTimes.entrySet()) {
          LOG.info(String
              .format("Service %s is healthy with an uptime of %dms", entry.getKey().toString(), entry.getValue()));
        }
      }

      @Override
      public void failure(Service service) {
        LOG.error(String.format("Service %s failed for the following reason:\n\t%s", service.toString(),
            service.failureCause().toString()));
        System.exit(1);
      }
    }, Executors.newSingleThreadExecutor(ExecutorsUtils.newThreadFactory(Optional.of(LOG))));

    // Add a shutdown hook so the task scheduler gets properly shutdown
    Runtime.getRuntime().addShutdownHook(new Thread() {

      public void run() {
        // Give the services 5 seconds to stop to ensure that we are
        // responsive to shutdown requests
        LOG.info("Shutting down the worker");
        try {
          serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
          LOG.error("Timeout in stopping the service manager", te);
        }
      }
    });

    LOG.info("Starting the worker with configured services");
    // Start the worker
    this.serviceManager.startAsync();
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: Worker <work configuration properties file>");
      System.exit(1);
    }

    // Load framework configuration properties
    Configuration config = new PropertiesConfiguration(args[0]);
    // Start the worker
    new Worker(ConfigurationConverter.getProperties(config)).start();
  }
}
