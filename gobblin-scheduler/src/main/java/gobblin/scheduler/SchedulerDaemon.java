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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import gobblin.configuration.ConfigurationKeys;
import gobblin.rest.JobExecutionInfoServer;


/**
 * A class that runs the {@link JobScheduler} in a daemon process for standalone deployment.
 *
 * @author ynli
 */
public class SchedulerDaemon {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerDaemon.class);

  private final ServiceManager serviceManager;

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final JmxReporter jmxReporter = JmxReporter.forRegistry(this.metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build();

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
        LOG.info("Shutting down the scheduler daemon");
        try {
          // Give the services 5 seconds to stop to ensure that we are responsive to shutdown requests
          serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
        } catch (TimeoutException te) {
          LOG.error("Timeout in stopping the service manager", te);
        }

        // Stop metric reporting
        jmxReporter.stop();
      }
    });

    // Register JVM metrics to collect and report
    registerJvmMetrics();
    // Start metric reporting
    this.jmxReporter.start();

    LOG.info("Starting the scheduler daemon");
    // Start the scheduler daemon
    this.serviceManager.startAsync();
  }

  private void registerJvmMetrics() {
    registerMetricSetWithPrefix("jvm.gc", new GarbageCollectorMetricSet());
    registerMetricSetWithPrefix("jvm.memory", new MemoryUsageGaugeSet());
    registerMetricSetWithPrefix("jvm.threads", new ThreadStatesGaugeSet());
    this.metricRegistry.register("jvm.fileDescriptorRatio", new FileDescriptorRatioGauge());
  }

  private void registerMetricSetWithPrefix(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      this.metricRegistry.register(MetricRegistry.name(prefix, entry.getKey()), entry.getValue());
    }
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
