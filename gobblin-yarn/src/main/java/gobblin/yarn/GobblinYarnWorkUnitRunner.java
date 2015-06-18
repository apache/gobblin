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

import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;

import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;


/**
 * The main class running in the containers managing services for running Gobblin
 * {@link gobblin.source.workunit.WorkUnit}s.
 *
 * <p>
 *   This class registers as a Helix participant upon startup.
 * </p>
 *
 * @author ynli
 */
public class GobblinYarnWorkUnitRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinYarnWorkUnitRunner.class);

  static final String GOBBLIN_TASK_FACTORY_NAME = "GobblinTaskFactory";

  private final ContainerId containerId;

  private final ServiceManager serviceManager;

  private final HelixManager helixManager;

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final JmxReporter jmxReporter = JmxReporter.forRegistry(this.metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build();

  public GobblinYarnWorkUnitRunner(String applicationName, Config config) throws Exception {
    this.containerId =
        ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));
    FileSystem fs = FileSystem.get(new Configuration());

    String zkConnectionString = config.getString(ConfigurationConstants.ZK_CONNECTION_STRING_KEY);
    this.helixManager = HelixManagerFactory.getZKHelixManager(
        config.hasPath(ConfigurationConstants.HELIX_CLUSTER_NAME_KEY) ?
            config.getString(ConfigurationConstants.HELIX_CLUSTER_NAME_KEY) : applicationName,
        YarnHelixUtils.getParticipantIdStr(YarnHelixUtils.getHostname(), this.containerId),
        InstanceType.PARTICIPANT, zkConnectionString);

    Properties properties = YarnHelixUtils.configToProperties(config);
    TaskExecutor taskExecutor = new TaskExecutor(properties);
    TaskStateTracker taskStateTracker = new YarnHelixTaskStateTracker(properties, this.helixManager);
    List<? extends Service> services = Lists.newArrayList(taskExecutor, taskStateTracker);
    this.serviceManager = new ServiceManager(services);

    StateMachineEngine stateMachineEngine = this.helixManager.getStateMachineEngine();
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(
        fs, applicationName, containerId.getApplicationAttemptId().getApplicationId());
    taskFactoryMap.put(GOBBLIN_TASK_FACTORY_NAME,
        new GobblinHelixTaskFactory(taskExecutor, taskStateTracker, fs, appWorkDir));
    stateMachineEngine.registerStateModelFactory(StateModelDefId.from("Task"),
        new TaskStateModelFactory(this.helixManager, taskFactoryMap));
  }

  /**
   * Start this {@link GobblinYarnWorkUnitRunner} instance.
   */
  public void start() {
    LOGGER.info(String.format("Starting %s in container %s",
        GobblinYarnWorkUnitRunner.class.getSimpleName(), this.containerId));

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

    this.serviceManager.startAsync();
    this.serviceManager.awaitStopped();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
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
    });
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

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", ConfigurationConstants.APPLICATION_NAME_OPTION_NAME, true, "Yarn application name");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinYarnApplicationMaster.class.getSimpleName(), options);
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
          GobblinYarnWorkUnitRunner.class, Log4jConfigurationHelper.LOG4J_CONFIGURATION_FILE_NAME);

      GobblinYarnWorkUnitRunner yarnWorkUnitRunner = new GobblinYarnWorkUnitRunner(
          cmd.getOptionValue(ConfigurationConstants.APPLICATION_NAME_OPTION_NAME), ConfigFactory.load());
      yarnWorkUnitRunner.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
