/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.runtime.app;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.rest.JobExecutionInfoServer;
import org.apache.gobblin.runtime.api.AdminWebServerFactory;
import org.apache.gobblin.runtime.services.JMXReportingService;
import org.apache.gobblin.runtime.services.MetricsReportingService;
import org.apache.gobblin.util.ApplicationLauncherUtils;
import org.apache.gobblin.util.ClassAliasResolver;


/**
 * An implementation of {@link ApplicationLauncher} that defines an application as a set of {@link Service}s that should
 * be started and stopped. The class will run a set of core services, some of which are optional, some of which are
 * mandatory. These {@link Service}s are as follows:
 *
 * <ul>
 *   <li>{@link MetricsReportingService} is optional and controlled by {@link ConfigurationKeys#METRICS_ENABLED_KEY}</li>
 *   <li>{@link JobExecutionInfoServer} is optional and controlled by {@link ConfigurationKeys#JOB_EXECINFO_SERVER_ENABLED_KEY}</li>
 *   <li>AdminWebServer is optional and controlled by {@link ConfigurationKeys#ADMIN_SERVER_ENABLED_KEY}</li>
 *   <li>{@link JMXReportingService} is mandatory</li>
 * </ul>
 *
 * <p>
 *   Additional {@link Service}s can be added via the {@link #addService(Service)} method. A {@link Service} cannot be
 *   added after the application has started. Additional {@link Service}s can also be specified via the configuration
 *   key {@link #APP_ADDITIONAL_SERVICES}.
 * </p>
 *
 * <p>
 *   An {@link ServiceBasedAppLauncher} cannot be restarted.
 * </p>
 */
@Alpha
public class ServiceBasedAppLauncher implements ApplicationLauncher {

  /**
   * The name of the application. Not applicable for YARN jobs, which uses a separate key for the application name.
   */
  public static final String APP_NAME = "app.name";

  /**
   * The number of seconds to wait for the application to stop, the default value is {@link #DEFAULT_APP_STOP_TIME_SECONDS}
   */
  public static final String APP_STOP_TIME_SECONDS = "app.stop.time.seconds";
  private static final String DEFAULT_APP_STOP_TIME_SECONDS = Long.toString(60);

  /**
   * A comma separated list of fully qualified classes that implement the {@link Service} interface. These
   * {@link Service}s will be run in addition to the core services.
   */
  public static final String APP_ADDITIONAL_SERVICES = "app.additional.services";

  private static final Logger LOG = LoggerFactory.getLogger(ServiceBasedAppLauncher.class);

  private final int stopTime;
  private final String appId;
  private final List<Service> services;

  private volatile boolean hasStarted = false;
  private volatile boolean hasStopped = false;

  private ServiceManager serviceManager;

  public ServiceBasedAppLauncher(Properties properties, String appName) throws Exception {
    this.stopTime = Integer.parseInt(properties.getProperty(APP_STOP_TIME_SECONDS, DEFAULT_APP_STOP_TIME_SECONDS));
    this.appId = ApplicationLauncherUtils.newAppId(appName);
    this.services = new ArrayList<>();

    // Add core Services needed for any application
    addJobExecutionServerAndAdminUI(properties);
    addMetricsService(properties);
    addJMXReportingService();

    // Add any additional Services specified via configuration keys
    addServicesFromProperties(properties);

    // Add a shutdown hook that interrupts the main thread
    addInterruptedShutdownHook();
  }

  /**
   * Starts the {@link ApplicationLauncher} by starting all associated services. This method also adds a shutdown hook
   * that invokes {@link #stop()} and the {@link #close()} methods. So {@link #stop()} and {@link #close()} need not be
   * called explicitly; they can be triggered during the JVM shutdown.
   */
  @Override
  public synchronized void start() {
    if (this.hasStarted) {
      LOG.warn("ApplicationLauncher has already started");
      return;
    }
    this.hasStarted = true;

    this.serviceManager = new ServiceManager(this.services);
    // A listener that shutdowns the application if any service fails.
    this.serviceManager.addListener(new ServiceManager.Listener() {
      @Override
      public void failure(Service service) {
        super.failure(service);
        LOG.error(String.format("Service %s has failed.", service.getClass().getSimpleName()), service.failureCause());
        try {
          service.stopAsync();
          ServiceBasedAppLauncher.this.stop();
        } catch (ApplicationException ae) {
          LOG.error("Could not shutdown services gracefully. This may cause the application to hang.");
        }
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
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
    this.serviceManager.startAsync().awaitHealthy();
  }

  /**
   * Stops the {@link ApplicationLauncher} by stopping all associated services.
   */
  @Override
  public synchronized void stop() throws ApplicationException {
    if (!this.hasStarted) {
      LOG.warn("ApplicationLauncher was never started");
      return;
    }
    if (this.hasStopped) {
      LOG.warn("ApplicationLauncher has already stopped");
      return;
    }
    this.hasStopped = true;

    LOG.info("Shutting down the application");
    try {
      this.serviceManager.stopAsync().awaitStopped(this.stopTime, TimeUnit.SECONDS);
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
    boolean adminUiServerEnabled =
        Boolean.valueOf(properties.getProperty(ConfigurationKeys.ADMIN_SERVER_ENABLED_KEY, Boolean.FALSE.toString()));

    if (jobExecInfoServerEnabled) {
      LOG.info("Will launch the job execution info server");
      JobExecutionInfoServer executionInfoServer = new JobExecutionInfoServer(properties);
      addService(executionInfoServer);

      if (adminUiServerEnabled) {
        LOG.info("Will launch the admin UI server");
        addService(createAdminServer(properties, executionInfoServer.getAdvertisedServerUri()));
      }
    } else if (adminUiServerEnabled) {
      LOG.warn("Not launching the admin UI because the job execution info server is not enabled");
    }
  }

  public static Service createAdminServer(Properties properties,
                                          URI executionInfoServerURI) {
    String factoryClassName = properties.getProperty(ConfigurationKeys.ADMIN_SERVER_FACTORY_CLASS_KEY,
                                                 ConfigurationKeys.DEFAULT_ADMIN_SERVER_FACTORY_CLASS);
    ClassAliasResolver<AdminWebServerFactory> classResolver =
        new ClassAliasResolver<>(AdminWebServerFactory.class);
    try
    {
      AdminWebServerFactory factoryInstance = classResolver.resolveClass(factoryClassName).newInstance();
      return factoryInstance.createInstance(properties, executionInfoServerURI);
    }
    catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException("Unable to instantiate the AdminWebServer factory. " +
           "Have you included the module in the gobblin distribution? :" + e, e);
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

  private void addServicesFromProperties(Properties properties)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException, InvocationTargetException {
    if (properties.containsKey(APP_ADDITIONAL_SERVICES)) {
      for (String serviceClassName : new State(properties).getPropAsSet(APP_ADDITIONAL_SERVICES)) {
        Class<?> serviceClass = Class.forName(serviceClassName);
        if (Service.class.isAssignableFrom(serviceClass)) {
          Service service;
          Constructor<?> constructor =
              ConstructorUtils.getMatchingAccessibleConstructor(serviceClass, Properties.class);
          if (constructor != null) {
            service = (Service) constructor.newInstance(properties);
          } else {
            service = (Service) serviceClass.newInstance();
          }
          addService(service);
        } else {
          throw new IllegalArgumentException(String.format("Class %s specified by %s does not implement %s",
              serviceClassName, APP_ADDITIONAL_SERVICES, Service.class.getSimpleName()));
        }
      }
    }
  }

  private void addInterruptedShutdownHook() {
    final Thread mainThread = Thread.currentThread();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        mainThread.interrupt();
      }
    });
  }
}
