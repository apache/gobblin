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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.ContainerHealthCheckException;
import org.apache.gobblin.cluster.ContainerHealthMetricsService;
import org.apache.gobblin.cluster.ContainerMetrics;
import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterManager;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.TaskRunnerSuiteBase;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MultiReporterException;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.reporter.util.MetricReportUtils;
import org.apache.gobblin.runtime.api.TaskEventMetadataGenerator;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.workflows.client.TemporalWorkflowClientFactory;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.FileUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.TaskEventMetadataUtils;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * The main class running in the containers managing services for running Gobblin
 * {@link org.apache.gobblin.source.workunit.WorkUnit}s.
 *
 * <p>
 *   If for some reason, the container exits or gets killed, the {@link GobblinClusterManager} will
 *   be notified for the completion of the container and will start a new container to replace this one.
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinTemporalTaskRunner implements StandardMetricsBridge {
  // Working directory key for applications. This config is set dynamically.
  public static final String CLUSTER_APP_WORK_DIR = GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX + "appWorkDir";

  private static final Logger logger = LoggerFactory.getLogger(GobblinTemporalTaskRunner.class);

  static final java.nio.file.Path CLUSTER_CONF_PATH = Paths.get("generated-gobblin-cluster.conf");

  private final Optional<ContainerMetrics> containerMetrics;
  private final Path appWorkPath;
  private boolean isTaskDriver;
  @Getter
  private volatile boolean started = false;
  private volatile boolean stopInProgress = false;
  private volatile boolean isStopped = false;
  @Getter
  @Setter
  private volatile boolean healthCheckFailed = false;

  protected final String taskRunnerId;
  protected final EventBus eventBus = new EventBus(GobblinTemporalTaskRunner.class.getSimpleName());
  protected final Config clusterConfig;
  @Getter
  protected final FileSystem fs;
  protected final String applicationName;
  protected final String applicationId;
  protected final int numTemporalWorkers;
  protected final String temporalQueueName;
  private final boolean isMetricReportingFailureFatal;
  private final boolean isEventReportingFailureFatal;

  public GobblinTemporalTaskRunner(String applicationName,
      String applicationId,
      String taskRunnerId,
      Config config,
      Optional<Path> appWorkDirOptional) throws Exception {
    GobblinClusterUtils.setSystemProperties(config);

    //Add dynamic config
    config = GobblinClusterUtils.addDynamicConfig(config);

    this.isTaskDriver = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.TASK_DRIVER_ENABLED,false);
    this.taskRunnerId = taskRunnerId;
    this.applicationName = applicationName;
    this.applicationId = applicationId;
    Configuration conf = HadoopUtils.newConfiguration();
    this.fs = GobblinClusterUtils.buildFileSystem(config, conf);
    this.appWorkPath = initAppWorkDir(config, appWorkDirOptional);
    this.clusterConfig = saveConfigToFile(config);

    logger.info("Configured GobblinTaskRunner work dir to: {}", this.appWorkPath.toString());

    this.containerMetrics = buildContainerMetrics();
    this.numTemporalWorkers = ConfigUtils.getInt(config, GobblinTemporalConfigurationKeys.TEMPORAL_NUM_WORKERS_PER_CONTAINER,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_WORKERS_PER_CONTAINERS);
    this.temporalQueueName = ConfigUtils.getString(config, GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
        GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE);

    this.isMetricReportingFailureFatal = ConfigUtils.getBoolean(this.clusterConfig,
        ConfigurationKeys.GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL,
        ConfigurationKeys.DEFAULT_GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL);

    this.isEventReportingFailureFatal = ConfigUtils.getBoolean(this.clusterConfig,
        ConfigurationKeys.GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL,
        ConfigurationKeys.DEFAULT_GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL);

    logger.info("GobblinTaskRunner({}): applicationName {}, applicationId {}, taskRunnerId {}, config {}, appWorkDir {}",
        this.isTaskDriver ? "taskDriver" : "worker",
        applicationName,
        applicationId,
        taskRunnerId,
        config,
        appWorkDirOptional);
  }

  public TaskRunnerSuiteBase.Builder getTaskRunnerSuiteBuilder() throws ReflectiveOperationException {
    String builderStr = ConfigUtils.getString(this.clusterConfig,
        GobblinClusterConfigurationKeys.TASK_RUNNER_SUITE_BUILDER,
        TaskRunnerSuiteBase.Builder.class.getName());

    String hostName = "";
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      logger.warn("Cannot find host name for Helix instance: {}");
    }

    TaskRunnerSuiteBase.Builder builder = GobblinConstructorUtils.<TaskRunnerSuiteBase.Builder>invokeLongestConstructor(
        new ClassAliasResolver(TaskRunnerSuiteBase.Builder.class)
            .resolveClass(builderStr), this.clusterConfig);

    return builder.setAppWorkPath(this.appWorkPath)
        .setContainerMetrics(this.containerMetrics)
        .setFileSystem(this.fs)
        .setApplicationId(applicationId)
        .setApplicationName(applicationName)
        .setContainerId(taskRunnerId)
        .setHostName(hostName);
  }

  private Path initAppWorkDir(Config config, Optional<Path> appWorkDirOptional) {
    return appWorkDirOptional.isPresent() ? appWorkDirOptional.get() : GobblinClusterUtils
        .getAppWorkDirPathFromConfig(config, this.fs, this.applicationName, this.applicationId);
  }

  private Config saveConfigToFile(Config config)
      throws IOException {
    Config newConf = config
        .withValue(CLUSTER_APP_WORK_DIR, ConfigValueFactory.fromAnyRef(this.appWorkPath.toString()));
    ConfigUtils configUtils = new ConfigUtils(new FileUtils());
    configUtils.saveConfigToFile(newConf, CLUSTER_CONF_PATH);
    return newConf;
  }

  /**
   * Start this {@link GobblinTemporalTaskRunner} instance.
   */
  public void start()
      throws ContainerHealthCheckException {
    logger.info("Calling start method in GobblinTemporalTaskRunner");
    logger.info(String.format("Starting in container %s", this.taskRunnerId));

    // Start metric reporting
    initMetricReporter();

    // Add a shutdown hook so the task scheduler gets properly shutdown
    addShutdownHook();

    try {
      for (int i = 0; i < this.numTemporalWorkers; i++) {
        initiateWorker();
      }
    }catch (Exception e) {
      logger.info(e + " for initiate workers");
      throw new RuntimeException(e);
    }
  }

  private void initiateWorker() throws Exception{
    logger.info("Starting Temporal Worker");

    String connectionUri = clusterConfig.getString(GobblinTemporalConfigurationKeys.TEMPORAL_CONNECTION_STRING);
    WorkflowServiceStubs service = TemporalWorkflowClientFactory.createServiceInstance(connectionUri);

    String namespace = ConfigUtils.getString(clusterConfig, GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_NAMESPACE,
            GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_NAMESPACE);
    WorkflowClient client = TemporalWorkflowClientFactory.createClientInstance(service, namespace);

    String workerClassName = ConfigUtils.getString(clusterConfig,
        GobblinTemporalConfigurationKeys.WORKER_CLASS, GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS);
    AbstractTemporalWorker worker = GobblinConstructorUtils.invokeLongestConstructor(
        (Class<AbstractTemporalWorker>) Class.forName(workerClassName), clusterConfig, client);
    worker.start();
    logger.info("A new worker is started.");
  }

  private void initMetricReporter() {
    if (this.containerMetrics.isPresent()) {
      try {
        this.containerMetrics.get()
            .startMetricReportingWithFileSuffix(ConfigUtils.configToState(this.clusterConfig), this.taskRunnerId);
      } catch (MultiReporterException ex) {
        if (MetricReportUtils.shouldThrowException(logger, ex, this.isMetricReportingFailureFatal, this.isEventReportingFailureFatal)) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  public synchronized void stop() {
    if (this.isStopped) {
      logger.info("Gobblin Task runner is already stopped.");
      return;
    }

    if (this.stopInProgress) {
      logger.info("Gobblin Task runner stop already in progress.");
      return;
    }

    this.stopInProgress = true;

    logger.info("Stopping the Gobblin Task runner");

    // Stop metric reporting
    if (this.containerMetrics.isPresent()) {
      this.containerMetrics.get().stopMetricsReporting();
    }

    logger.info("All services are stopped.");

    this.isStopped = true;
  }

  /**
   * Creates and returns a {@link List} of additional {@link Service}s that should be run in this
   * {@link GobblinTemporalTaskRunner}. Sub-classes that need additional {@link Service}s to run, should override this method
   *
   * @return a {@link List} of additional {@link Service}s to run.
   */
  protected List<Service> getServices() {
    List<Service> serviceList = new ArrayList<>();
    if (ConfigUtils.getBoolean(this.clusterConfig, GobblinClusterConfigurationKeys.CONTAINER_HEALTH_METRICS_SERVICE_ENABLED,
        GobblinClusterConfigurationKeys.DEFAULT_CONTAINER_HEALTH_METRICS_SERVICE_ENABLED)) {
      serviceList.add(new ContainerHealthMetricsService(clusterConfig));
    }
    return serviceList;
  }

  @VisibleForTesting
  boolean isStopped() {
    return this.isStopped;
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        logger.info("Running the shutdown hook");
        GobblinTemporalTaskRunner.this.stop();
      }
    });
  }

  private Optional<ContainerMetrics> buildContainerMetrics() {
    Properties properties = ConfigUtils.configToProperties(this.clusterConfig);
    if (GobblinMetrics.isEnabled(properties)) {
      logger.info("Container metrics are enabled");
      return Optional.of(ContainerMetrics
          .get(ConfigUtils.configToState(clusterConfig), this.applicationName, this.taskRunnerId));
    } else {
      return Optional.absent();
    }
  }

  // hard coded for now
  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return null;
  }

  @Subscribe
  public void handleContainerHealthCheckFailureEvent(ContainerHealthCheckFailureEvent event) {
    logger.error("Received {} from: {}", event.getClass().getSimpleName(), event.getClassName());
    logger.error("Submitting a ContainerHealthCheckFailureEvent..");
    submitEvent(event);
    logger.error("Stopping GobblinTaskRunner...");
    GobblinTemporalTaskRunner.this.setHealthCheckFailed(true);
    GobblinTemporalTaskRunner.this.stop();
  }

  private void submitEvent(ContainerHealthCheckFailureEvent event) {
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(RootMetricContext.get(), getClass().getPackage().getName()).build();
    GobblinEventBuilder eventBuilder = new GobblinEventBuilder(event.getClass().getSimpleName());
    State taskState = ConfigUtils.configToState(event.getConfig());
    //Add task metadata such as taskId, containerId, and workflowId if configured
    TaskEventMetadataGenerator taskEventMetadataGenerator = TaskEventMetadataUtils.getTaskEventMetadataGenerator(taskState);
    eventBuilder.addAdditionalMetadata(taskEventMetadataGenerator.getMetadata(taskState, event.getClass().getSimpleName()));
    eventBuilder.addAdditionalMetadata(event.getMetadata());
    eventSubmitter.submit(eventBuilder);
  }

  private static String getApplicationId() {
    return "1";
  }

  private static String getTaskRunnerId() {
    return UUID.randomUUID().toString();
  }

  public static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true,
        "Application name");
    options.addOption("d", GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME, true,
        "Application id");
    options.addOption("i", GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME, true,
        "Helix instance name");
    options.addOption(Option.builder("t").longOpt(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_OPTION_NAME)
        .hasArg(true).required(false).desc("Helix instance tags").build());
    return options;
  }

  public static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinClusterManager.class.getSimpleName(), options);
  }

  public static void main(String[] args)
      throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      logger.info(JvmUtils.getJvmInputArguments());

      String applicationName =
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME);
      GobblinTemporalTaskRunner gobblinWorkUnitRunner =
          new GobblinTemporalTaskRunner(applicationName, getApplicationId(),
              getTaskRunnerId(), ConfigFactory.load(), Optional.<Path>absent());
      gobblinWorkUnitRunner.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}

