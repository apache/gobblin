/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.LiveInstance;
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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.yarn.event.ApplicationMasterShutdownRequest;


/**
 * The Yarn ApplicationMaster class for Gobblin.
 *
 * <p>
 *   The Yarn ApplicationMaster runs the {@link GobblinHelixJobScheduler} for scheduling and running
 *   Gobblin jobs, and the {@link YarnService} for all Yarn-related stuffs.
 * </p>
 *
 * @author ynli
 */
public class GobblinApplicationMaster {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinApplicationMaster.class);

  private final ServiceManager serviceManager;

  private final HelixManager helixManager;

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final JmxReporter jmxReporter = JmxReporter.forRegistry(this.metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build();

  public GobblinApplicationMaster(String applicationName, Config config) throws Exception {
    ContainerId containerId =
        ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));
    ApplicationAttemptId applicationAttemptIdId = containerId.getApplicationAttemptId();

    String zkConnectionString = config.hasPath(ConfigurationConstants.ZK_CONNECTION_STRING_KEY) ?
        config.getString(ConfigurationConstants.ZK_CONNECTION_STRING_KEY) :
        ConfigurationConstants.DEFAULT_ZK_CONNECTION_STRING;

    // This will create and register a Helix controller in ZooKeeper
    this.helixManager = HelixManagerFactory.getZKHelixManager(
        config.getString(ConfigurationConstants.HELIX_CLUSTER_NAME_KEY),
        YarnHelixUtils.getParticipantIdStr(YarnHelixUtils.getHostname(), containerId),
        InstanceType.CONTROLLER, zkConnectionString);
    this.helixManager.addLiveInstanceChangeListener(new GobblinLiveInstanceChangeListener());

    // An EventBus used for communications between services running in the ApplicationMaster
    EventBus eventBus = new EventBus(GobblinApplicationMaster.class.getSimpleName());
    eventBus.register(this);

    FileSystem fs = FileSystem.get(new Configuration());
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(fs, applicationName, applicationAttemptIdId.getApplicationId());

    List<Service> services = Lists.newArrayList();
    if (UserGroupInformation.isSecurityEnabled()) {
      LOGGER.info("Adding YarnAMSecurityManager since security is enabled");
      services.add(new ControllerSecurityManager(config, fs));
    }
    services.add(new YarnService(config, applicationName, applicationAttemptIdId.getApplicationId(), eventBus));
    services.add(new GobblinHelixJobScheduler(YarnHelixUtils.configToProperties(config), this.helixManager,
        eventBus, appWorkDir));
    services.add(new JobConfigurationManager(eventBus,
        config.hasPath(ConfigurationConstants.JOB_CONF_PACKAGE_PATH_KEY) ?
            Optional.of(config.getString(ConfigurationConstants.JOB_CONF_PACKAGE_PATH_KEY)) :
            Optional.<String>absent()));

    this.serviceManager = new ServiceManager(services);
  }

  /**
   * Start this scheduler daemon.
   */
  public void start() throws IOException, YarnException {
    LOGGER.info("Starting the Gobblin Yarn ApplicationMaster");

    // Add a shutdown hook so the task scheduler gets properly shutdown
    addShutdownHook();

    try {
      this.helixManager.connect();
    } catch (Exception e) {
      throw new RuntimeException("The HelixManager failed to connect", e);
    }

    // Register JVM metrics to collect and report
    registerJvmMetrics();
    // Start metric reporting
    this.jmxReporter.start();

    // Start all the services running in the ApplicationMaster
    this.serviceManager.startAsync();
    this.serviceManager.awaitHealthy();
  }

  public void stop() {
    LOGGER.info("Shutting down the Gobblin Yarn ApplicationMaster");
    try {
      // Give the services 5 seconds to stop to ensure that we are responsive to shutdown requests
      serviceManager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      LOGGER.error("Timeout in stopping the service manager", te);
    } finally {
      // Stop metric reporting
      jmxReporter.stop();

      helixManager.disconnect();
    }
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleApplicationMasterShutdownRequest(ApplicationMasterShutdownRequest shutdownRequest) {
    stop();
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

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        GobblinApplicationMaster.this.stop();
      }
    });
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", ConfigurationConstants.APPLICATION_NAME_OPTION_NAME, true, "Yarn application name");
    return options;
  }

  /**
   * A custom implementation of {@link LiveInstanceChangeListener}.
   */
  private class GobblinLiveInstanceChangeListener implements LiveInstanceChangeListener {

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      for (LiveInstance liveInstance : liveInstances) {
        LOGGER.info("Live Helix participant instance: " + liveInstance.getInstanceName());
      }
    }
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinApplicationMaster.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(ConfigurationConstants.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigurationHelper.updateLog4jConfiguration(
          GobblinApplicationMaster.class, Log4jConfigurationHelper.LOG4J_CONFIGURATION_FILE_NAME);

      GobblinApplicationMaster applicationMaster = new GobblinApplicationMaster(
          cmd.getOptionValue(ConfigurationConstants.APPLICATION_NAME_OPTION_NAME), ConfigFactory.load());
      applicationMaster.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
