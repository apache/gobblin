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

package com.linkedin.uif.scheduler;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.gobblin.rest.JobExecutionInfoServer;


/**
 * A class that runs the {@link JobScheduler} in a daemon process for standalone deployment.
 *
 * @author ynli
 */
public class SchedulerDaemon {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerDaemon.class);

  private final ServiceManager serviceManager;

  public SchedulerDaemon(Properties properties)
      throws Exception {
    List<Service> services = Lists.<Service>newArrayList(new JobScheduler(properties));
    boolean jobExecInfoServerEnabled = Boolean
        .valueOf(properties.getProperty(ConfigurationKeys.JOB_EXECINFO_SERVER_ENABLED_KEY, Boolean.FALSE.toString()));
    if (jobExecInfoServerEnabled) {
      services.add(new JobExecutionInfoServer(properties));
    }
    this.serviceManager = new ServiceManager(services);
  }

  /**
   * Start this scheduler daemon.
   */
  public void start() {
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

  public static void main(String[] args)
      throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: SchedulerDaemon <configuration properties file>");
      System.exit(1);
    }

    // Load framework configuration properties
    Configuration config = new PropertiesConfiguration(args[0]);
    // Start the scheduler daemon
    new SchedulerDaemon(ConfigurationConverter.getProperties(config)).start();
  }
}
