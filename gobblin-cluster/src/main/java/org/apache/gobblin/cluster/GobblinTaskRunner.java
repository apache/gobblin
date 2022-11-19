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

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MultiTypeMessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
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
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.FileUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.TaskEventMetadataUtils;
import org.apache.gobblin.util.event.ContainerHealthCheckFailureEvent;
import org.apache.gobblin.util.eventbus.EventBusFactory;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

/**
 * The main class running in the containers managing services for running Gobblin
 * {@link org.apache.gobblin.source.workunit.WorkUnit}s.
 *
 * <p>
 *   This class presents a Helix participant that uses a {@link HelixManager} to communicate with Helix.
 *   It uses Helix task execution framework and details are encapsulated in {@link TaskRunnerSuiteBase}.
 * </p>
 *
 * <p>
 *   This class responds to a graceful shutdown initiated by the {@link GobblinClusterManager} via
 *   a Helix message of subtype {@link HelixMessageSubTypes#WORK_UNIT_RUNNER_SHUTDOWN}, or it does a
 *   graceful shutdown when the shutdown hook gets called. In both cases, {@link #stop()} will be
 *   called to start the graceful shutdown.
 * </p>
 *
 * <p>
 *   If for some reason, the container exits or gets killed, the {@link GobblinClusterManager} will
 *   be notified for the completion of the container and will start a new container to replace this one.
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinTaskRunner implements StandardMetricsBridge {
  // Working directory key for applications. This config is set dynamically.
  public static final String CLUSTER_APP_WORK_DIR = GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_PREFIX + "appWorkDir";

  private static final Logger logger = LoggerFactory.getLogger(GobblinTaskRunner.class);

  static final java.nio.file.Path CLUSTER_CONF_PATH = Paths.get("generated-gobblin-cluster.conf");
  static final String GOBBLIN_TASK_FACTORY_NAME = "GobblinTaskFactory";
  static final String GOBBLIN_JOB_FACTORY_NAME = "GobblinJobFactory";

  private final String helixInstanceName;
  private final String clusterName;
  private final Optional<ContainerMetrics> containerMetrics;
  private final List<Service> services = Lists.newArrayList();
  private final Path appWorkPath;
  //An EventBus instance that can be accessed from any component running within the worker process. The individual components can
  // use the EventBus stream to communicate back application level health check results to the
  // GobblinTaskRunner.
  private final EventBus containerHealthEventBus;

  @Getter
  private HelixManager jobHelixManager;
  private Optional<HelixManager> taskDriverHelixManager = Optional.absent();
  private ServiceManager serviceManager;
  private TaskStateModelFactory taskStateModelFactory;
  private boolean isTaskDriver;
  private boolean dedicatedTaskDriverCluster;
  private boolean isContainerExitOnHealthCheckFailureEnabled;

  private Collection<StandardMetricsBridge.StandardMetrics> metricsCollection;
  @Getter
  private volatile boolean started = false;
  private volatile boolean stopInProgress = false;
  private volatile boolean isStopped = false;
  @Getter
  @Setter
  private volatile boolean healthCheckFailed = false;

  protected final String taskRunnerId;
  protected final EventBus eventBus = new EventBus(GobblinTaskRunner.class.getSimpleName());
  protected final Config clusterConfig;
  @Getter
  protected final FileSystem fs;
  protected final String applicationName;
  protected final String applicationId;
  private final boolean isMetricReportingFailureFatal;
  private final boolean isEventReportingFailureFatal;

  public GobblinTaskRunner(String applicationName,
      String helixInstanceName,
      String applicationId,
      String taskRunnerId,
      Config config,
      Optional<Path> appWorkDirOptional) throws Exception {
    // Set system properties passed in via application config. As an example, Helix uses System#getProperty() for ZK configuration
    // overrides such as sessionTimeout. In this case, the overrides specified
    // in the application configuration have to be extracted and set before initializing HelixManager.
    GobblinClusterUtils.setSystemProperties(config);

    //Add dynamic config
    config = GobblinClusterUtils.addDynamicConfig(config);

    this.isTaskDriver = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.TASK_DRIVER_ENABLED,false);
    this.helixInstanceName = helixInstanceName;
    this.taskRunnerId = taskRunnerId;
    this.applicationName = applicationName;
    this.applicationId = applicationId;
    this.dedicatedTaskDriverCluster = ConfigUtils.getBoolean(config,
        GobblinClusterConfigurationKeys.DEDICATED_TASK_DRIVER_CLUSTER_ENABLED, false);
    Configuration conf = HadoopUtils.newConfiguration();
    this.fs = GobblinClusterUtils.buildFileSystem(config, conf);
    this.appWorkPath = initAppWorkDir(config, appWorkDirOptional);
    this.clusterConfig = saveConfigToFile(config);
    this.clusterName = this.clusterConfig.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    this.isMetricReportingFailureFatal = ConfigUtils.getBoolean(this.clusterConfig,
        ConfigurationKeys.GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL,
        ConfigurationKeys.DEFAULT_GOBBLIN_TASK_METRIC_REPORTING_FAILURE_FATAL);

    this.isEventReportingFailureFatal = ConfigUtils.getBoolean(this.clusterConfig,
        ConfigurationKeys.GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL,
        ConfigurationKeys.DEFAULT_GOBBLIN_TASK_EVENT_REPORTING_FAILURE_FATAL);

    logger.info("Configured GobblinTaskRunner work dir to: {}", this.appWorkPath.toString());

    this.isContainerExitOnHealthCheckFailureEnabled = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.CONTAINER_EXIT_ON_HEALTH_CHECK_FAILURE_ENABLED,
        GobblinClusterConfigurationKeys.DEFAULT_CONTAINER_EXIT_ON_HEALTH_CHECK_FAILURE_ENABLED);

    if (this.isContainerExitOnHealthCheckFailureEnabled) {
      EventBus eventBus;
      try {
        eventBus = EventBusFactory.get(ContainerHealthCheckFailureEvent.CONTAINER_HEALTH_CHECK_EVENT_BUS_NAME,
            SharedResourcesBrokerFactory.getImplicitBroker());
      } catch (IOException e) {
        logger.error("Could not find EventBus instance for container health check", e);
        eventBus = null;
      }
      this.containerHealthEventBus = eventBus;
    } else {
      this.containerHealthEventBus = null;
    }

    initHelixManager();

    this.containerMetrics = buildContainerMetrics();

    logger.info("GobblinTaskRunner({}): applicationName {}, helixInstanceName {}, applicationId {}, taskRunnerId {}, config {}, appWorkDir {}",
        this.isTaskDriver ? "taskDriver" : "worker",
        applicationName,
        helixInstanceName,
        applicationId,
        taskRunnerId,
        config,
        appWorkDirOptional);
  }

  private TaskRunnerSuiteBase initTaskRunnerSuiteBase() throws ReflectiveOperationException {
    String builderStr = ConfigUtils.getString(this.clusterConfig,
        GobblinClusterConfigurationKeys.TASK_RUNNER_SUITE_BUILDER,
        TaskRunnerSuiteBase.Builder.class.getName());

    String hostName = "";
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      logger.warn("Cannot find host name for Helix instance: {}", this.helixInstanceName);
    }

    TaskRunnerSuiteBase.Builder builder = GobblinConstructorUtils.<TaskRunnerSuiteBase.Builder>invokeLongestConstructor(
        new ClassAliasResolver(TaskRunnerSuiteBase.Builder.class)
            .resolveClass(builderStr), this.clusterConfig);

    return builder.setAppWorkPath(this.appWorkPath)
        .setContainerMetrics(this.containerMetrics)
        .setFileSystem(this.fs)
        .setJobHelixManager(this.jobHelixManager)
        .setApplicationId(applicationId)
        .setApplicationName(applicationName)
        .setInstanceName(helixInstanceName)
        .setContainerId(taskRunnerId)
        .setHostName(hostName)
        .build();
  }

  private Path initAppWorkDir(Config config, Optional<Path> appWorkDirOptional) {
    return appWorkDirOptional.isPresent() ? appWorkDirOptional.get() : GobblinClusterUtils
        .getAppWorkDirPathFromConfig(config, this.fs, this.applicationName, this.applicationId);
  }

  private void initHelixManager() {
    String zkConnectionString =
        this.clusterConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    logger.info("Using ZooKeeper connection string: " + zkConnectionString);

    if (this.isTaskDriver && this.dedicatedTaskDriverCluster) {
      // This will create a Helix manager to receive the planning job
      this.taskDriverHelixManager = Optional.of(GobblinHelixManagerFactory.getZKHelixManager(
          ConfigUtils.getString(this.clusterConfig, GobblinClusterConfigurationKeys.TASK_DRIVER_CLUSTER_NAME_KEY, ""),
          this.helixInstanceName,
          InstanceType.PARTICIPANT,
          zkConnectionString));
      this.jobHelixManager = GobblinHelixManagerFactory.getZKHelixManager(
          this.clusterName,
          this.helixInstanceName,
          InstanceType.ADMINISTRATOR,
          zkConnectionString);
    } else {
      this.jobHelixManager = GobblinHelixManagerFactory.getZKHelixManager(
          this.clusterName,
          this.helixInstanceName,
          InstanceType.PARTICIPANT,
          zkConnectionString);
    }
  }

  private HelixManager getReceiverManager() {
    return taskDriverHelixManager.isPresent() ? taskDriverHelixManager.get() : this.jobHelixManager;
  }

  private TaskStateModelFactory createTaskStateModelFactory(Map<String, TaskFactory> taskFactoryMap) {
    HelixManager receiverManager = getReceiverManager();
    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(receiverManager, taskFactoryMap);
    receiverManager.getStateMachineEngine()
        .registerStateModelFactory("Task", taskStateModelFactory);
    return taskStateModelFactory;
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
   * Start this {@link GobblinTaskRunner} instance.
   */
  public void start()
      throws ContainerHealthCheckException {
    logger.info(String.format("Starting %s in container %s", this.helixInstanceName, this.taskRunnerId));

    // Add a shutdown hook so the task scheduler gets properly shutdown
    addShutdownHook();

    connectHelixManagerWithRetry();

    TaskRunnerSuiteBase suite;
    try {
      suite = initTaskRunnerSuiteBase();
      synchronized (this) {
        this.taskStateModelFactory = createTaskStateModelFactory(suite.getTaskFactoryMap());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    this.metricsCollection = suite.getMetricsCollection();
    this.services.addAll(suite.getServices());

    this.services.addAll(getServices());

    if (this.services.isEmpty()) {
      this.serviceManager = null;
    } else {
      this.serviceManager = new ServiceManager(services);
    }

    addInstanceTags();

    // Start metric reporting
    initMetricReporter();

    if (this.containerHealthEventBus != null) {
      //Register itself with the container health event bus instance to receive container health events
      logger.info("Registering GobblinTaskRunner with ContainerHealthCheckEventBus..");
      this.containerHealthEventBus.register(this);
    }

    if (this.serviceManager != null) {
      this.serviceManager.startAsync();
      started = true;
      this.serviceManager.awaitStopped();
    } else {
      started = true;
    }

    //Check if the TaskRunner shutdown is invoked due to a health check failure. If yes, throw a RuntimeException
    // that will be propagated to the caller.
    if (this.isContainerExitOnHealthCheckFailureEnabled && GobblinTaskRunner.this.isHealthCheckFailed()) {
      logger.error("GobblinTaskRunner finished due to health check failure.");
      throw new ContainerHealthCheckException();
    }
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

    try {
      stopServices();
    } finally {
      logger.info("All services are stopped.");
      this.taskStateModelFactory.shutdown();
      disconnectHelixManager();
    }

    this.isStopped = true;
  }

  private void stopServices() {
    if (this.serviceManager != null) {
      try {
        // Give the services 5 minutes to stop to ensure that we are responsive to shutdown requests
        this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.MINUTES);
      } catch (TimeoutException te) {
        logger.error("Timeout in stopping the service manager", te);
      }
    }
  }

  /**
   * Creates and returns a {@link List} of additional {@link Service}s that should be run in this
   * {@link GobblinTaskRunner}. Sub-classes that need additional {@link Service}s to run, should override this method
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

  @VisibleForTesting
  void connectHelixManager() throws Exception {
    this.jobHelixManager.connect();
    if (!(this.isTaskDriver && this.dedicatedTaskDriverCluster)) {
      // Ensure the instance is enabled when jobHelixManager is a PARTICIPANT
      this.jobHelixManager.getClusterManagmentTool().enableInstance(clusterName, helixInstanceName, true);
    }
    this.jobHelixManager.getMessagingService()
        .registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
            new ParticipantShutdownMessageHandlerFactory());
    this.jobHelixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
            getUserDefinedMessageHandlerFactory());
    if (this.taskDriverHelixManager.isPresent()) {
      this.taskDriverHelixManager.get().connect();
      //Ensure the instance is enabled.
      this.taskDriverHelixManager.get().getClusterManagmentTool().enableInstance(this.taskDriverHelixManager.get().getClusterName(), helixInstanceName, true);
    }
  }

  /**
   * A method to handle failures joining Helix cluster. The method will perform the following steps before attempting
   * to re-join the cluster:
   * <li>
   *   <ul>Disconnect from Helix cluster, which would close any open clients</ul>
   *   <ul>Drop instance from Helix cluster, to remove any partial instance structure from Helix</ul>
   *   <ul>Re-construct helix manager instances, used to re-join the cluster</ul>
   * </li>
   */
  private void onClusterJoinFailure() {
    logger.warn("Disconnecting Helix manager..");
    disconnectHelixManager();

    HelixAdmin admin = new ZKHelixAdmin(clusterConfig.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY));
    //Drop the helix Instance
    logger.warn("Dropping instance: {} from cluster: {}", helixInstanceName, clusterName);
    HelixUtils.dropInstanceIfExists(admin, clusterName, helixInstanceName);

    if (this.taskDriverHelixManager.isPresent()) {
      String taskDriverCluster = clusterConfig.getString(GobblinClusterConfigurationKeys.TASK_DRIVER_CLUSTER_NAME_KEY);
      logger.warn("Dropping instance: {} from task driver cluster: {}", helixInstanceName, taskDriverCluster);
      HelixUtils.dropInstanceIfExists(admin, clusterName, helixInstanceName);
    }
    admin.close();

    logger.warn("Reinitializing Helix manager..");
    initHelixManager();
  }

  @VisibleForTesting
  void connectHelixManagerWithRetry() {
    Callable<Void> connectHelixManagerCallable = () -> {
      try {
        logger.info("Instance: {} attempting to join cluster: {}", helixInstanceName, clusterName);
        connectHelixManager();
      } catch (HelixException e) {
        logger.error("Exception encountered when joining cluster", e);
        onClusterJoinFailure();
        throw e;
      }
      return null;
    };

    Retryer<Void> retryer = RetryerBuilder.<Void>newBuilder()
        .retryIfException()
        .withStopStrategy(StopStrategies.stopAfterAttempt(5)).build();

    try {
      retryer.call(connectHelixManagerCallable);
    } catch (ExecutionException | RetryException e) {
      Throwables.propagate(e);
    }
  }

  /**
   * Helix participant cannot pre-configure tags before it connects to ZK. So this method can only be invoked after
   * {@link HelixManager#connect()}. However this will still work because tagged jobs won't be sent to a non-tagged instance. Hence
   * the job with EXAMPLE_INSTANCE_TAG will remain in the ZK until an instance with EXAMPLE_INSTANCE_TAG was found.
   */
  private void addInstanceTags() {
    HelixManager receiverManager = getReceiverManager();
    if (receiverManager.isConnected()) {
      try {
        Set<String> desiredTags = new HashSet<>(
            ConfigUtils.getStringList(this.clusterConfig, GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY));
        if (!desiredTags.isEmpty()) {
          // The helix instance associated with this container should be consistent on helix tag
          List<String> existedTags = receiverManager.getClusterManagmentTool()
              .getInstanceConfig(this.clusterName, this.helixInstanceName).getTags();
          // Remove tag assignments for the current Helix instance from a previous run
          for (String tag : existedTags) {
            if (!desiredTags.contains(tag)) {
              receiverManager.getClusterManagmentTool().removeInstanceTag(this.clusterName, this.helixInstanceName, tag);
              logger.info("Removed unrelated helix tag {} for instance {}", tag, this.helixInstanceName);
            }
          }
          desiredTags.forEach(desiredTag -> receiverManager.getClusterManagmentTool()
              .addInstanceTag(this.clusterName, this.helixInstanceName, desiredTag));
          logger.info("Actual tags binding " + receiverManager.getClusterManagmentTool()
              .getInstanceConfig(this.clusterName, this.helixInstanceName).getTags());
        }
      } catch (HelixException e) {
        logger.warn("Error with Helix getting instance config tags used in YARN cluster configuration. Ensure YARN is being used. Will ignore and attempt to move on {}", e);
      }
    }
  }

  /**
   * Creates and returns a {@link MultiTypeMessageHandlerFactory} for handling of Helix
   * {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}s.
   *
   * @returns a {@link MultiTypeMessageHandlerFactory}.
   */
  protected MultiTypeMessageHandlerFactory getUserDefinedMessageHandlerFactory() {
    return new ParticipantUserDefinedMessageHandlerFactory();
  }

  @VisibleForTesting
  void disconnectHelixManager() {
    if (this.jobHelixManager.isConnected()) {
      this.jobHelixManager.disconnect();
    }

    if (this.taskDriverHelixManager.isPresent()) {
      this.taskDriverHelixManager.get().disconnect();
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        logger.info("Running the shutdown hook");
        GobblinTaskRunner.this.stop();
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

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return this.metricsCollection;
  }

  /**
   * A custom {@link MultiTypeMessageHandlerFactory} for {@link ParticipantShutdownMessageHandler}s that handle messages
   * of type "SHUTDOWN" for shutting down the participants.
   */
  private class ParticipantShutdownMessageHandlerFactory implements MultiTypeMessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ParticipantShutdownMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE;
    }

    public List<String> getMessageTypes() {
      return Collections.singletonList(getMessageType());
    }

    @Override
    public void reset() {

    }

    /**
     * A custom {@link MessageHandler} for handling messages of sub type
     * {@link HelixMessageSubTypes#WORK_UNIT_RUNNER_SHUTDOWN}.
     */
    private class ParticipantShutdownMessageHandler extends MessageHandler {

      public ParticipantShutdownMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() {
        String messageSubType = this._message.getMsgSubType();
        Preconditions.checkArgument(messageSubType
            .equalsIgnoreCase(HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString()), String
            .format("Unknown %s message subtype: %s", GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE, messageSubType));

        HelixTaskResult result = new HelixTaskResult();

        if (stopInProgress) {
          result.setSuccess(true);
          return result;
        }

        logger.info("Handling message " + HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString());

        ScheduledExecutorService shutdownMessageHandlingCompletionWatcher =
            MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1));

        // Schedule the task for watching on the removal of the shutdown message, which indicates that
        // the message has been successfully processed and it's safe to disconnect the HelixManager.
        // This is a hacky way of watching for the completion of processing the shutdown message and
        // should be replaced by a fix to https://issues.apache.org/jira/browse/HELIX-611.
        shutdownMessageHandlingCompletionWatcher.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            HelixManager helixManager = _notificationContext.getManager();
            HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();

            HelixProperty helixProperty = helixDataAccessor.getProperty(
                _message.getKey(helixDataAccessor.keyBuilder(), helixManager.getInstanceName()));
            // The absence of the shutdown message indicates it has been removed
            if (helixProperty == null) {
              GobblinTaskRunner.this.stop();
            }
          }
        }, 0, 1, TimeUnit.SECONDS);

        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        logger.error(String
            .format("Failed to handle message with exception %s, error code %s, error type %s", e,
                code, type));
      }
    }
  }

  /**
   * A custom {@link MultiTypeMessageHandlerFactory} for {@link ParticipantUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private static class ParticipantUserDefinedMessageHandlerFactory implements MultiTypeMessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ParticipantUserDefinedMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return Message.MessageType.USER_DEFINE_MSG.toString();
    }

    public List<String> getMessageTypes() {
      return Collections.singletonList(getMessageType());
    }

    @Override
    public void reset() {

    }

    /**
     * A custom {@link MessageHandler} for handling user-defined messages to the controller.
     *
     * <p>
     *   Currently does not handle any user-defined messages. If this class is passed a custom message, it will simply
     *   print out a warning and return successfully. Sub-classes of {@link GobblinClusterManager} should override
     *   {@link #getUserDefinedMessageHandlerFactory}.
     * </p>
     */
    private static class ParticipantUserDefinedMessageHandler extends MessageHandler {

      public ParticipantUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() {
        logger.warn(String.format("No handling setup for %s message of subtype: %s",
            Message.MessageType.USER_DEFINE_MSG.toString(), this._message.getMsgSubType()));

        HelixTaskResult helixTaskResult = new HelixTaskResult();
        helixTaskResult.setSuccess(true);
        return helixTaskResult;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        logger.error(String
            .format("Failed to handle message with exception %s, error code %s, error type %s", e,
                code, type));
      }
    }
  }

  @Subscribe
  public void handleContainerHealthCheckFailureEvent(ContainerHealthCheckFailureEvent event) {
    logger.error("Received {} from: {}", event.getClass().getSimpleName(), event.getClassName());
    logger.error("Submitting a ContainerHealthCheckFailureEvent..");
    submitEvent(event);
    logger.error("Stopping GobblinTaskRunner...");
    GobblinTaskRunner.this.setHealthCheckFailed(true);
    GobblinTaskRunner.this.stop();
  }

  private void submitEvent(ContainerHealthCheckFailureEvent event) {
    EventSubmitter eventSubmitter = new EventSubmitter.Builder(RootMetricContext.get(), getClass().getPackage().getName()).build();
    GobblinEventBuilder eventBuilder = new GobblinEventBuilder(event.getClass().getSimpleName());
    State taskState = ConfigUtils.configToState(event.getConfig());
    //Add task metadata such as Helix taskId, containerId, and workflowId if configured
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
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME) || !cmd
          .hasOption(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      logger.info(JvmUtils.getJvmInputArguments());

      String applicationName =
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME);
      String helixInstanceName =
          cmd.getOptionValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME);

      GobblinTaskRunner gobblinWorkUnitRunner =
          new GobblinTaskRunner(applicationName, helixInstanceName, getApplicationId(),
              getTaskRunnerId(), ConfigFactory.load(), Optional.<Path>absent());
      gobblinWorkUnitRunner.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}