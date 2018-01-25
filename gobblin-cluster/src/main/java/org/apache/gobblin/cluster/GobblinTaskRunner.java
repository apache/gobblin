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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.services.JMXReportingService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.FileUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.PathUtils;

import static org.apache.gobblin.cluster.GobblinClusterConfigurationKeys.CLUSTER_WORK_DIR;


/**
 * The main class running in the containers managing services for running Gobblin
 * {@link org.apache.gobblin.source.workunit.WorkUnit}s.
 *
 * <p>
 *   This class serves as a Helix participant and it uses a {@link HelixManager} to work with Helix.
 *   This class also uses the Helix task execution framework and {@link GobblinHelixTaskFactory} class
 *   for creating {@link GobblinHelixTask}s that Helix manages to run Gobblin data ingestion tasks.
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
public class GobblinTaskRunner {

  private static final Logger logger = LoggerFactory.getLogger(GobblinTaskRunner.class);
  static final java.nio.file.Path CLUSTER_CONF_PATH = Paths.get("generated-gobblin-cluster.conf");

  static final String GOBBLIN_TASK_FACTORY_NAME = "GobblinTaskFactory";

  private final String helixInstanceName;

  private HelixManager helixManager;

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

  public GobblinTaskRunner(String applicationName, String helixInstanceName, String applicationId,
      String taskRunnerId, Config config, Optional<Path> appWorkDirOptional)
      throws Exception {
    this.helixInstanceName = helixInstanceName;
    this.taskRunnerId = taskRunnerId;
    this.applicationName = applicationName;
    this.applicationId = applicationId;

    Configuration conf = HadoopUtils.newConfiguration();
    this.fs = buildFileSystem(config, conf);

    this.appWorkPath = initAppWorkDir(config, appWorkDirOptional);

    this.config = saveConfigToFile(config);

    initHelixManager();

    this.containerMetrics = buildContainerMetrics();

    this.taskStateModelFactory = registerHelixTaskFactory();

    services.addAll(getServices());
    this.serviceManager = new ServiceManager(services);

    logger.debug("GobblinTaskRunner: applicationName {}, helixInstanceName {}, applicationId {}, taskRunnerId {}, config {}, appWorkDir {}",
        applicationName, helixInstanceName, applicationId, taskRunnerId, config, appWorkDirOptional);
  }

  private Path initAppWorkDir(Config config, Optional<Path> appWorkDirOptional) {
    return appWorkDirOptional.isPresent() ? appWorkDirOptional.get() : GobblinClusterUtils
        .getAppWorkDirPathFromConfig(config, this.fs, this.applicationName, this.applicationId);
  }

  private void initHelixManager() {
    String zkConnectionString =
        this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    logger.info("Using ZooKeeper connection string: " + zkConnectionString);

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY),
        this.helixInstanceName, InstanceType.PARTICIPANT, zkConnectionString);
  }

  private TaskStateModelFactory registerHelixTaskFactory() {
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();

    boolean isRunTaskInSeparateProcessEnabled = getIsRunTaskInSeparateProcessEnabled();
    TaskFactory taskFactory;
    if (isRunTaskInSeparateProcessEnabled) {
      logger.info("Running a task in a separate process is enabled.");
      taskFactory = new HelixTaskFactory(this.containerMetrics, CLUSTER_CONF_PATH, config);
    } else {
      taskFactory = getInProcessTaskFactory();
    }

    taskFactoryMap.put(GOBBLIN_TASK_FACTORY_NAME, taskFactory);
    TaskStateModelFactory taskStateModelFactory =
        new TaskStateModelFactory(this.helixManager, taskFactoryMap);
    this.helixManager.getStateMachineEngine()
        .registerStateModelFactory("Task", taskStateModelFactory);
    return taskStateModelFactory;
  }

  private TaskFactory getInProcessTaskFactory() {
    Properties properties = ConfigUtils.configToProperties(this.config);
    URI rootPathUri = PathUtils.getRootPath(this.appWorkPath).toUri();
    Config stateStoreJobConfig = ConfigUtils.propertiesToConfig(properties)
        .withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY,
            ConfigValueFactory.fromAnyRef(rootPathUri.toString()));

    TaskExecutor taskExecutor = new TaskExecutor(properties);
    TaskStateTracker taskStateTracker = new GobblinHelixTaskStateTracker(properties);

    services.add(taskExecutor);
    services.add(taskStateTracker);
    services.add(new JMXReportingService(
        ImmutableMap.of("task.executor", taskExecutor.getTaskExecutorQueueMetricSet())));

    TaskFactory taskFactory =
        new GobblinHelixTaskFactory(this.containerMetrics, taskExecutor, taskStateTracker, this.fs,
            this.appWorkPath, stateStoreJobConfig, this.helixManager);
    return taskFactory;
  }

  private Boolean getIsRunTaskInSeparateProcessEnabled() {
    Boolean enabled = false;
    if (this.config.hasPath(GobblinClusterConfigurationKeys.ENABLE_TASK_IN_SEPARATE_PROCESS)) {
      enabled =
          this.config.getBoolean(GobblinClusterConfigurationKeys.ENABLE_TASK_IN_SEPARATE_PROCESS);
    }
    return enabled;
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
    logger.info(
        String.format("Starting %s in container %s", this.helixInstanceName, this.taskRunnerId));

    // Add a shutdown hook so the task scheduler gets properly shutdown
    addShutdownHook();

    connectHelixManager();

    // Start metric reporting
    if (this.containerMetrics.isPresent()) {
      this.containerMetrics.get()
          .startMetricReportingWithFileSuffix(ConfigUtils.configToState(this.config),
              this.taskRunnerId);
    }

    this.serviceManager.startAsync();
    this.serviceManager.awaitStopped();
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
      // Give the services 5 minutes to stop to ensure that we are responsive to shutdown requests
      this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.MINUTES);
    } catch (TimeoutException te) {
      logger.error("Timeout in stopping the service manager", te);
    } finally {
      this.taskStateModelFactory.shutdown();

      disconnectHelixManager();
    }

    this.isStopped = true;
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
      this.helixManager.connect();
      this.helixManager.getMessagingService()
          .registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
              new ParticipantShutdownMessageHandlerFactory());
      this.helixManager.getMessagingService()
          .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
              getUserDefinedMessageHandlerFactory());
    } catch (Exception e) {
      logger.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
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
    if (this.helixManager.isConnected()) {
      this.helixManager.disconnect();
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
