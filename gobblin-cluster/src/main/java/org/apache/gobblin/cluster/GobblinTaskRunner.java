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
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
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
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.FileUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

import static org.apache.gobblin.cluster.GobblinClusterConfigurationKeys.CLUSTER_WORK_DIR;


/**
 * The main class running in the containers managing services for running Gobblin
 * {@link org.apache.gobblin.source.workunit.WorkUnit}s.
 *
 * <p>
 *   This class presents a Helix participant and uses a {@link HelixManager} to communicate with Helix.
 *   It also uses Helix task execution framework and {@link GobblinHelixTaskFactory} class to generate
 *   {@link GobblinHelixTask}s which handles real Gobblin tasks. All the Helix related task framework is
 *   encapsulated in {@link TaskRunnerSuiteBase}.
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

  private static final Logger logger = LoggerFactory.getLogger(GobblinTaskRunner.class);
  static final java.nio.file.Path CLUSTER_CONF_PATH = Paths.get("generated-gobblin-cluster.conf");

  static final String GOBBLIN_TASK_FACTORY_NAME = "GobblinTaskFactory";

  static final String GOBBLIN_JOB_FACTORY_NAME = "GobblinJobFactory";

  private final String helixInstanceName;

  private final String clusterName;

  private HelixManager jobHelixManager;

  private Optional<HelixManager> taskDriverHelixManager = Optional.absent();

  private final ServiceManager serviceManager;

  private final TaskStateModelFactory taskStateModelFactory;

  private final Optional<ContainerMetrics> containerMetrics;

  private final String taskRunnerId;

  private volatile boolean stopInProgress = false;

  private volatile boolean isStopped = false;

  protected final EventBus eventBus = new EventBus(GobblinTaskRunner.class.getSimpleName());

  protected final Config config;

  protected final FileSystem fs;
  private final List<Service> services = Lists.newArrayList();
  private final String applicationName;
  private final String applicationId;
  private final Path appWorkPath;
  private boolean isTaskDriver;
  private boolean dedicatedTaskDriverCluster;

  private final Collection<StandardMetricsBridge.StandardMetrics> metricsCollection;

  public GobblinTaskRunner(String applicationName,
      String helixInstanceName,
      String applicationId,
      String taskRunnerId,
      Config config,
      Optional<Path> appWorkDirOptional) throws Exception {
    this.isTaskDriver = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.TASK_DRIVER_ENABLED,false);
    this.helixInstanceName = helixInstanceName;
    this.taskRunnerId = taskRunnerId;
    this.applicationName = applicationName;
    this.applicationId = applicationId;
    this.dedicatedTaskDriverCluster = ConfigUtils.getBoolean(config,
        GobblinClusterConfigurationKeys.DEDICATED_TASK_DRIVER_CLUSTER_ENABLED, false);
    Configuration conf = HadoopUtils.newConfiguration();
    this.fs = buildFileSystem(config, conf);
    this.appWorkPath = initAppWorkDir(config, appWorkDirOptional);
    this.config = saveConfigToFile(config);
    this.clusterName = this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);

    initHelixManager();

    this.containerMetrics = buildContainerMetrics();

    String builderStr = ConfigUtils.getString(this.config,
        GobblinClusterConfigurationKeys.TASK_RUNNER_SUITE_BUILDER,
        TaskRunnerSuiteBase.Builder.class.getName());

    TaskRunnerSuiteBase.Builder builder = GobblinConstructorUtils.<TaskRunnerSuiteBase.Builder>invokeLongestConstructor(
          new ClassAliasResolver(TaskRunnerSuiteBase.Builder.class)
              .resolveClass(builderStr), this.config);

    TaskRunnerSuiteBase suite = builder.setAppWorkPath(this.appWorkPath)
        .setContainerMetrics(this.containerMetrics)
        .setFileSystem(this.fs)
        .setJobHelixManager(this.jobHelixManager)
        .setApplicationId(applicationId)
        .setApplicationName(applicationName)
        .setInstanceName(helixInstanceName)
        .build();

    this.taskStateModelFactory = createTaskStateModelFactory(suite.getTaskFactoryMap());
    this.metricsCollection = suite.getMetricsCollection();
    this.services.addAll(suite.getServices());

    this.services.addAll(getServices());
    if (this.services.isEmpty()) {
      this.serviceManager = null;
    } else {
      this.serviceManager = new ServiceManager(services);
    }

    logger.info("GobblinTaskRunner({}): applicationName {}, helixInstanceName {}, applicationId {}, taskRunnerId {}, config {}, appWorkDir {}",
        this.isTaskDriver?"taskDriver" : "worker",
        applicationName,
        helixInstanceName,
        applicationId,
        taskRunnerId,
        config,
        appWorkDirOptional);
  }

  private Path initAppWorkDir(Config config, Optional<Path> appWorkDirOptional) {
    return appWorkDirOptional.isPresent() ? appWorkDirOptional.get() : GobblinClusterUtils
        .getAppWorkDirPathFromConfig(config, this.fs, this.applicationName, this.applicationId);
  }

  private void initHelixManager() {
    String zkConnectionString =
        this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    logger.info("Using ZooKeeper connection string: " + zkConnectionString);

    if (this.isTaskDriver && this.dedicatedTaskDriverCluster) {
      // This will create a Helix manager to receive the planning job
      this.taskDriverHelixManager = Optional.of(HelixManagerFactory.getZKHelixManager(
          ConfigUtils.getString(this.config, GobblinClusterConfigurationKeys.TASK_DRIVER_CLUSTER_NAME_KEY, ""),
          this.helixInstanceName,
          InstanceType.PARTICIPANT,
          zkConnectionString));
      this.jobHelixManager = HelixManagerFactory.getZKHelixManager(
          this.clusterName,
          this.helixInstanceName,
          InstanceType.ADMINISTRATOR,
          zkConnectionString);
    } else {
      this.jobHelixManager = HelixManagerFactory.getZKHelixManager(
          this.clusterName,
          this.helixInstanceName,
          InstanceType.PARTICIPANT,
          zkConnectionString);
    }
  }

  private TaskStateModelFactory createTaskStateModelFactory(Map<String, TaskFactory> taskFactoryMap) {
    HelixManager receiverManager = taskDriverHelixManager.isPresent()?taskDriverHelixManager.get()
        : this.jobHelixManager;
    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(receiverManager, taskFactoryMap);
    receiverManager.getStateMachineEngine()
        .registerStateModelFactory("Task", taskStateModelFactory);
    return taskStateModelFactory;
  }

  private Config saveConfigToFile(Config config)
      throws IOException {
    Config newConf = config
        .withValue(CLUSTER_WORK_DIR, ConfigValueFactory.fromAnyRef(this.appWorkPath.toString()));
    ConfigUtils configUtils = new ConfigUtils(new FileUtils());
    configUtils.saveConfigToFile(newConf, CLUSTER_CONF_PATH);
    return newConf;
  }

  /**
   * Start this {@link GobblinTaskRunner} instance.
   */
  public void start() {
    logger.info(String.format("Starting %s in container %s", this.helixInstanceName, this.taskRunnerId));

    // Add a shutdown hook so the task scheduler gets properly shutdown
    addShutdownHook();

    connectHelixManager();

    addInstanceTags();

    // Start metric reporting
    if (this.containerMetrics.isPresent()) {
      this.containerMetrics.get()
          .startMetricReportingWithFileSuffix(ConfigUtils.configToState(this.config),
              this.taskRunnerId);
    }

    if (this.serviceManager != null) {
      this.serviceManager.startAsync();
      this.serviceManager.awaitStopped();
    }
  }

  public synchronized void stop() {
    if (this.isStopped || this.stopInProgress) {
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
    return new ArrayList<>();
  }

  @VisibleForTesting
  boolean isStopped() {
    return this.isStopped;
  }

  @VisibleForTesting
  void connectHelixManager() {
    try {
      this.jobHelixManager.connect();
      this.jobHelixManager.getMessagingService()
          .registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
              new ParticipantShutdownMessageHandlerFactory());
      this.jobHelixManager.getMessagingService()
          .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
              getUserDefinedMessageHandlerFactory());
      if (this.taskDriverHelixManager.isPresent()) {
        this.taskDriverHelixManager.get().connect();
      }
    } catch (Exception e) {
      logger.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Helix participant cannot pre-configure tags before it connects to ZK. So this method can only be invoked after
   * {@link HelixManager#connect()}. However this will still work because tagged jobs won't be sent to a non-tagged instance. Hence
   * the job with EXAMPLE_INSTANCE_TAG will remain in the ZK until an instance with EXAMPLE_INSTANCE_TAG was found.
   */
  private void addInstanceTags() {
    List<String> tags = ConfigUtils.getStringList(this.config, GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY);
    HelixManager receiverManager = taskDriverHelixManager.isPresent()?taskDriverHelixManager.get()
        : this.jobHelixManager;
    if (receiverManager.isConnected()) {
      if (!tags.isEmpty()) {
        logger.info("Adding tags binding " + tags);
        tags.forEach(tag -> receiverManager.getClusterManagmentTool()
            .addInstanceTag(this.clusterName, this.helixInstanceName, tag));
        logger.info("Actual tags binding " + receiverManager.getClusterManagmentTool()
            .getInstanceConfig(this.clusterName, this.helixInstanceName).getTags());
      }
    }
  }

  /**
   * Creates and returns a {@link MessageHandlerFactory} for handling of Helix
   * {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}s.
   *
   * @returns a {@link MessageHandlerFactory}.
   */
  protected MessageHandlerFactory getUserDefinedMessageHandlerFactory() {
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

  private FileSystem buildFileSystem(Config config, Configuration conf)
      throws IOException {
    return config.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem
        .get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), conf)
        : FileSystem.get(conf);
  }

  private Optional<ContainerMetrics> buildContainerMetrics() {
    Properties properties = ConfigUtils.configToProperties(this.config);
    if (GobblinMetrics.isEnabled(properties)) {
      return Optional.of(ContainerMetrics
          .get(ConfigUtils.configToState(config), this.applicationName, this.taskRunnerId));
    } else {
      return Optional.absent();
    }
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return this.metricsCollection;
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ParticipantShutdownMessageHandler}s that handle messages
   * of type "SHUTDOWN" for shutting down the participants.
   */
  private class ParticipantShutdownMessageHandlerFactory implements MessageHandlerFactory {

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
      public HelixTaskResult handleMessage()
          throws InterruptedException {
        String messageSubType = this._message.getMsgSubType();
        Preconditions.checkArgument(messageSubType
            .equalsIgnoreCase(HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString()), String
            .format("Unknown %s message subtype: %s", GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
                messageSubType));

        HelixTaskResult result = new HelixTaskResult();

        if (stopInProgress) {
          result.setSuccess(true);
          return result;
        }

        logger
            .info("Handling message " + HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString());

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
   * A custom {@link MessageHandlerFactory} for {@link ParticipantUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private static class ParticipantUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

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
      public HelixTaskResult handleMessage()
          throws InterruptedException {
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
    options.addOption("i", GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME, true,
        "Helix instance name");
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
