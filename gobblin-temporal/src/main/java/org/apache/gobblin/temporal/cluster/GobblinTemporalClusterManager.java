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

package org.apache.gobblin.temporal.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.ContainerHealthMetricsService;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterMetricTagNames;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.GobblinTaskRunner;
import org.apache.gobblin.cluster.JobConfigurationManager;
import org.apache.gobblin.cluster.LeadershipChangeAwareComponent;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobScheduler;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * The central cluster manager for Gobblin Clusters.
 */
@Alpha
@Slf4j
public class GobblinTemporalClusterManager implements ApplicationLauncher, StandardMetricsBridge, LeadershipChangeAwareComponent {

  private StopStatus stopStatus = new StopStatus(false);

  protected ServiceBasedAppLauncher applicationLauncher;

  // An EventBus used for communications between services running in the ApplicationMaster
  @Getter(AccessLevel.PUBLIC)
  protected final EventBus eventBus = new EventBus(GobblinTemporalClusterManager.class.getSimpleName());

  protected final Path appWorkDir;

  @Getter
  protected final FileSystem fs;

  protected final String applicationId;

  @Getter
  private MutableJobCatalog jobCatalog;
  @Getter
  private JobConfigurationManager jobConfigurationManager;
  @Getter
  private GobblinTemporalJobScheduler gobblinTemporalJobScheduler;
  @Getter
  private volatile boolean started = false;

  protected final String clusterName;
  @Getter
  protected final Config config;

  public GobblinTemporalClusterManager(String clusterName, String applicationId, Config sysConfig,
      Optional<Path> appWorkDirOptional) throws Exception {
    // Set system properties passed in via application config.
    // overrides such as sessionTimeout. In this case, the overrides specified
    GobblinClusterUtils.setSystemProperties(sysConfig);

    //Add dynamic config
    this.config = GobblinClusterUtils.addDynamicConfig(sysConfig);

    this.clusterName = clusterName;
    this.applicationId = applicationId;

    this.fs = GobblinClusterUtils.buildFileSystem(this.config, new Configuration());
    this.appWorkDir = appWorkDirOptional.isPresent() ? appWorkDirOptional.get()
        : GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, this.fs, clusterName, applicationId);
    log.info("Configured GobblinTemporalClusterManager work dir to: {}", this.appWorkDir);

    initializeAppLauncherAndServices();
  }

  /**
   * Create the service based application launcher and other associated services
   * @throws Exception
   */
  private void initializeAppLauncherAndServices() throws Exception {
    // Done to preserve backwards compatibility with the previously hard-coded timeout of 5 minutes
    Properties properties = ConfigUtils.configToProperties(this.config);
    if (!properties.contains(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS)) {
      properties.setProperty(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS, Long.toString(300));
    }
    this.applicationLauncher = new ServiceBasedAppLauncherWithoutMetrics(properties, this.clusterName);

    // create a job catalog for keeping track of received jobs if a job config path is specified
    if (this.config.hasPath(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX
        + ConfigurationKeys.JOB_CONFIG_FILE_GENERAL_PATH_KEY)) {
      String jobCatalogClassName = ConfigUtils.getString(config, GobblinClusterConfigurationKeys.JOB_CATALOG_KEY,
          GobblinClusterConfigurationKeys.DEFAULT_JOB_CATALOG);

      this.jobCatalog =
          (MutableJobCatalog) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(jobCatalogClassName),
          ImmutableList.of(config
              .getConfig(StringUtils.removeEnd(GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX, "."))
              .withFallback(this.config)));
    } else {
      this.jobCatalog = null;
    }

    SchedulerService schedulerService = new SchedulerService(properties);
    this.applicationLauncher.addService(schedulerService);
    this.gobblinTemporalJobScheduler = buildGobblinTemporalJobScheduler(config, this.appWorkDir, getMetadataTags(clusterName, applicationId),
        schedulerService);
    this.applicationLauncher.addService(this.gobblinTemporalJobScheduler);
    this.jobConfigurationManager = buildJobConfigurationManager(config);
    this.applicationLauncher.addService(this.jobConfigurationManager);

    if (ConfigUtils.getBoolean(this.config, GobblinClusterConfigurationKeys.CONTAINER_HEALTH_METRICS_SERVICE_ENABLED,
        GobblinClusterConfigurationKeys.DEFAULT_CONTAINER_HEALTH_METRICS_SERVICE_ENABLED)) {
      this.applicationLauncher.addService(new ContainerHealthMetricsService(config));
    }
  }

  /**
   * Start any services required by the application launcher then start the application launcher
   */
  private void startAppLauncherAndServices() {
    // other services such as the job configuration manager have a dependency on the job catalog, so it has be be
    // started first
    if (this.jobCatalog instanceof Service) {
      ((Service) this.jobCatalog).startAsync().awaitRunning();
    }

    this.applicationLauncher.start();
  }

  /**
   * Stop the application launcher then any services that were started outside of the application launcher
   */
  private void stopAppLauncherAndServices() {
    try {
      log.info("Stopping the Gobblin cluster application launcher");
      this.applicationLauncher.stop();
    } catch (ApplicationException ae) {
      log.error("Error while stopping Gobblin Cluster application launcher", ae);
    }

    log.info("Stopping the Gobblin cluster job catalog");
    if (this.jobCatalog instanceof Service) {
      ((Service) this.jobCatalog).stopAsync().awaitTerminated();
    }
    log.info("Stopped the Gobblin cluster job catalog");
  }


  /**
   * Start the Gobblin Temporal Cluster Manager.
   */
  @Override
  public void start() {
    // temporal workflow
    log.info("Starting the Gobblin Temporal Cluster Manager");

    this.eventBus.register(this);
    startAppLauncherAndServices();
    this.started = true;
  }

  /**
   * Stop the Gobblin Cluster Manager.
   */
  @Override
  public synchronized void stop() {
    if (this.stopStatus.isStopInProgress()) {
      return;
    }

    this.stopStatus.setStopInprogress(true);

    log.info("Stopping the Gobblin Temporal Cluster Manager");

    stopAppLauncherAndServices();
  }

  private GobblinTemporalJobScheduler buildGobblinTemporalJobScheduler(Config sysConfig, Path appWorkDir,
      List<? extends Tag<?>> metadataTags, SchedulerService schedulerService) throws Exception {
    return new GobblinTemporalJobScheduler(sysConfig,
        this.eventBus,
        appWorkDir,
        metadataTags,
        schedulerService);
  }

  private List<? extends Tag<?>> getMetadataTags(String applicationName, String applicationId) {
    return Tag.fromMap(
        new ImmutableMap.Builder<String, Object>().put(GobblinClusterMetricTagNames.APPLICATION_NAME, applicationName)
            .put(GobblinClusterMetricTagNames.APPLICATION_ID, applicationId).build());
  }

  /**
   * Build the {@link JobConfigurationManager} for the Application Master.
   */
  private JobConfigurationManager buildJobConfigurationManager(Config config) {
    try {
      List<Object> argumentList = (this.jobCatalog != null)? ImmutableList.of(this.eventBus, config, this.jobCatalog, this.fs) :
          ImmutableList.of(this.eventBus, config, this.fs);
      if (config.hasPath(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY)) {
        return (JobConfigurationManager) GobblinConstructorUtils.invokeLongestConstructor(Class.forName(
            config.getString(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY)), argumentList.toArray(new Object[argumentList.size()]));
      } else {
        return new JobConfigurationManager(this.eventBus, config);
      }
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleApplicationMasterShutdownRequest(ClusterManagerShutdownRequest shutdownRequest) {
    stop();
  }

  @Override
  public void close() throws IOException {
    this.applicationLauncher.close();
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    List<StandardMetrics> list = new ArrayList();
    list.addAll(this.jobCatalog.getStandardMetricsCollection());
    list.addAll(this.jobConfigurationManager.getStandardMetricsCollection());
    return list;
  }

  /**
   * comment lifted from {@link org.apache.gobblin.cluster.GobblinClusterManager}
   * TODO for now the cluster id is hardcoded to 1 both here and in the {@link GobblinTaskRunner}. In the future, the
   * cluster id should be created by the {@link GobblinTemporalClusterManager} and passed to each {@link GobblinTaskRunner}
   */
  private static String getApplicationId() {
    return "1";
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Gobblin application name");
    options.addOption("s", GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE, true, "Standalone cluster mode");
    options.addOption("i", GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME, true, "Helix instance name");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinTemporalClusterManager.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      boolean isStandaloneClusterManager = false;
      if (cmd.hasOption(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE)) {
        isStandaloneClusterManager = Boolean.parseBoolean(cmd.getOptionValue(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE, "false"));
      }

      log.info(JvmUtils.getJvmInputArguments());
      Config config = ConfigFactory.load();

      if (cmd.hasOption(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)) {
        config = config.withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
            ConfigValueFactory.fromAnyRef(cmd.getOptionValue(
                GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)));
      }

      if (isStandaloneClusterManager) {
        config = config.withValue(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE_KEY,
            ConfigValueFactory.fromAnyRef(true));
      }

      try (GobblinTemporalClusterManager GobblinTemporalClusterManager = new GobblinTemporalClusterManager(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME), getApplicationId(),
          config, Optional.<Path>absent())) {
        GobblinTemporalClusterManager.start();
      }
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }

  @Override
  public void becomeActive() {
    startAppLauncherAndServices();
  }

  @Override
  public void becomeStandby() {
    stopAppLauncherAndServices();
    try {
      initializeAppLauncherAndServices();
    } catch (Exception e) {
      throw new RuntimeException("Exception reinitializing app launcher services ", e);
    }
  }

  static class StopStatus {
    @Getter
    @Setter
    AtomicBoolean isStopInProgress;
    public StopStatus(boolean inProgress) {
      isStopInProgress = new AtomicBoolean(inProgress);
    }
    public void setStopInprogress (boolean inProgress) {
      isStopInProgress.set(inProgress);
    }
    public boolean isStopInProgress () {
      return isStopInProgress.get();
    }
  }
}

