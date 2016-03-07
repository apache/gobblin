/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime.app;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.admin.AdminWebServer;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.rest.JobExecutionInfoServer;
import gobblin.runtime.services.JMXReportingService;
import gobblin.runtime.services.MetricsReportingService;
import gobblin.util.ApplicationLauncherUtils;


/**
 * An implementation of {@link ApplicationLauncher} that defines an application as a set of {@link Service}s that should
 * be started and stopped. The class will run a set of core services, some of which are optional, some of which are
 * mandatory. These {@link Service}s are as follows:
 *
 * <ul>
 *   <li>{@link MetricsReportingService} is optional and controlled by {@link ConfigurationKeys#METRICS_ENABLED_KEY}</li>
 *   <li>{@link JobExecutionInfoServer} is optional and controlled by {@link ConfigurationKeys#JOB_EXECINFO_SERVER_ENABLED_KEY}</li>
 *   <li>{@link AdminWebServer} is optional and controlled by {@link ConfigurationKeys#ADMIN_SERVER_ENABLED_KEY}</li>
 *   <li>{@link JMXReportingService} is mandatory</li>
 * </ul>
 *
 * <p>
 *   Additional {@link Service}s can be added via the {@link #addService(Service)} method. A {@link Service} cannot be
 *   added after the application has started.
 * </p>
 */
public class ServiceBasedAppLauncher implements ApplicationLauncher {

  /**
   * The name of the application. Not applicable for YARN jobs, which uses a separate key for the application name.
   */
  public static final String APP_NAME = "app.name";

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBasedAppLauncher.class);

  private final String appId;
  private final List<Service> services;

  private volatile boolean hasStarted = false;
  private volatile boolean hasStopped = false;

  private ServiceManager serviceManager;

  public ServiceBasedAppLauncher(Properties properties, String appName) throws Exception {
    this.appId = ApplicationLauncherUtils.newAppId(appName);
    this.services = new ArrayList<>();

    addJobExecutionServerAndAdminUI(properties);
    addMetricsService(properties);
    addJMXReportingService();

    // Add a shutdown hook that interrupts the main thread
    addInterruptedShutdownHook();
  }

  /**
   * Starts the {@link ApplicationLauncher} by starting all associated services. This method also adds a shutdown hook
   * that invokes {@link #stop()} and the {@link #close()} methods. So {@link #stop()} and {@link #close()} need not be
   * called explicitly; they can be triggered during the JVM shutdown.
   */
  @Override
  public void start() {
    if (this.hasStarted) {
      LOG.warn("ApplicationLauncher has already started");
      return;
    } else {
      this.hasStarted = true;
    }

    this.serviceManager = new ServiceManager(this.services);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          ServiceBasedAppLauncher.this.stop();
        } catch (ApplicationException e) {
          LOG.error("Failed to shutdown application", e);
        } finally {
          try {
            ServiceBasedAppLauncher.this.close();
          } catch (IOException e) {
            LOG.error("Failed to close application", e);
          }
        }
      }
    });

    LOG.info("Starting the Gobblin application and all its associated Services");

    // Start the application
    this.serviceManager.startAsync();
  }

  /**
   * Stops the {@link ApplicationLauncher} by stopping all associated services.
   */
  @Override
  public void stop() throws ApplicationException {
    if (this.hasStopped) {
      LOG.warn("ApplicationLauncher has already stopped");
      return;
    } else {
      this.hasStopped = true;
    }

    LOG.info("Shutting down the application");
    try {
      // Give the services 5 seconds to stop to ensure that we are responsive to shutdown requests
      this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      LOG.error("Timeout in stopping the service manager", te);
    }
  }

  @Override
  public void close() throws IOException {
    // Do nothing
  }

  /**
   * Add a {@link Service} to be run by this {@link ApplicationLauncher}.
   *
   * <p>
   *   This method is public because there are certain classes launchers (such as Azkaban) that require the
   *   {@link ApplicationLauncher} to extend a pre-defined class. Since Java classes cannot extend multiple classes,
   *   composition needs to be used. In which case this method needs to be public.
   * </p>
   */
  public void addService(Service service) {
    if (this.hasStarted) {
      throw new IllegalArgumentException("Cannot add a service while the application is running!");
    }
    this.services.add(service);
  }

  private void addJobExecutionServerAndAdminUI(Properties properties) {

    boolean jobExecInfoServerEnabled = Boolean
        .valueOf(properties.getProperty(ConfigurationKeys.JOB_EXECINFO_SERVER_ENABLED_KEY, Boolean.FALSE.toString()));
    boolean adminUiServerEnabled = Boolean
        .valueOf(properties.getProperty(ConfigurationKeys.ADMIN_SERVER_ENABLED_KEY, Boolean.FALSE.toString()));

    if (jobExecInfoServerEnabled) {
      LOG.info("Will launch the job execution info server");
      JobExecutionInfoServer executionInfoServer = new JobExecutionInfoServer(properties);
      addService(executionInfoServer);

      if (adminUiServerEnabled) {
        LOG.info("Will launch the admin UI server");
        addService(new AdminWebServer(properties, executionInfoServer.getAdvertisedServerUri()));
      }
    }  else if (adminUiServerEnabled) {
      LOG.warn("Not launching the admin UI because the job execution info server is not enabled");
    }
  }

  private void addMetricsService(Properties properties) {
    if (GobblinMetrics.isEnabled(properties)) {
      addService(new MetricsReportingService(properties, this.appId));
    }
  }

  private void addJMXReportingService() {
    addService(new JMXReportingService());
  }

  private void addInterruptedShutdownHook() {
    final Thread mainThread = Thread.currentThread();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override public void run() {
        mainThread.interrupt();
      }
    });
  }
}
