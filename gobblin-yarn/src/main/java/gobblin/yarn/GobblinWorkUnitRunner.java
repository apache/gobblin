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

package gobblin.yarn;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;

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
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.GobblinMetrics;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskStateTracker;
import gobblin.runtime.services.JMXReportingService;
import gobblin.util.ConfigUtils;
import gobblin.util.HadoopUtils;
import gobblin.yarn.event.DelegationTokenUpdatedEvent;


/**
 * The main class running in the containers managing services for running Gobblin
 * {@link gobblin.source.workunit.WorkUnit}s.
 *
 * <p>
 *   This class serves as a Helix participant and it uses a {@link HelixManager} to work with Helix.
 *   This class also uses the Helix task execution framework and {@link GobblinHelixTaskFactory} class
 *   for creating {@link GobblinHelixTask}s that Helix manages to run Gobblin data ingestion tasks.
 * </p>
 *
 * <p>
 *   This class responds to a graceful shutdown initiated by the {@link GobblinApplicationMaster} via
 *   a Helix message of subtype {@link HelixMessageSubTypes#WORK_UNIT_RUNNER_SHUTDOWN}, or it does a
 *   graceful shutdown when the shutdown hook gets called. In both cases, {@link #stop()} will be
 *   called to start the graceful shutdown.
 * </p>
 *
 * <p>
 *   If for some reason, the container exits or gets killed, the {@link GobblinApplicationMaster} will
 *   be notified for the completion of the container and will start a new container to replace this one.
 * </p>
 *
 * @author Yinan Li
 */
public class GobblinWorkUnitRunner extends GobblinYarnLogSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinWorkUnitRunner.class);

  static final String GOBBLIN_TASK_FACTORY_NAME = "GobblinTaskFactory";

  private final String helixInstanceName;

  private final Config config;

  private final ContainerId containerId;

  private final HelixManager helixManager;

  private final ServiceManager serviceManager;

  private final EventBus eventBus = new EventBus(GobblinWorkUnitRunner.class.getSimpleName());

  private final TaskStateModelFactory taskStateModelFactory;

  private final Optional<ContainerMetrics> containerMetrics;

  private volatile boolean stopInProgress = false;
  private volatile boolean isStopped = false;

  public GobblinWorkUnitRunner(String applicationName, String helixInstanceName, ContainerId containerId, Config config,
      Optional<Path> appWorkDirOptional) throws Exception {
    this.helixInstanceName = helixInstanceName;
    this.config = config;
    this.containerId = containerId;

    ApplicationAttemptId applicationAttemptId = this.containerId.getApplicationAttemptId();
    String applicationId = applicationAttemptId.getApplicationId().toString();

    Configuration conf = HadoopUtils.newConfiguration();
    FileSystem fs = buildFileSystem(this.config, conf);

    String zkConnectionString = config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);

    this.helixManager = HelixManagerFactory
        .getZKHelixManager(config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY), helixInstanceName,
            InstanceType.PARTICIPANT, zkConnectionString);

    Properties properties = ConfigUtils.configToProperties(config);

    TaskExecutor taskExecutor = new TaskExecutor(properties);
    TaskStateTracker taskStateTracker = new GobblinHelixTaskStateTracker(properties, this.helixManager);

    Path appWorkDir = appWorkDirOptional.isPresent() ? appWorkDirOptional.get() :
        YarnHelixUtils.getAppWorkDirPath(fs, applicationName, applicationId);

    List<Service> services = Lists.newArrayList();
    if (isLogSourcePresent()) {
      services.add(buildLogCopier(config, this.containerId, fs, appWorkDir));
    }
    services.add(taskExecutor);
    services.add(taskStateTracker);
    if (config.hasPath(GobblinYarnConfigurationKeys.KEYTAB_FILE_PATH)) {
      LOGGER.info("Adding YarnContainerSecurityManager since login is keytab based");
      services.add(new YarnContainerSecurityManager(config, fs, this.eventBus));
    }
    services.add(new JMXReportingService());

    this.serviceManager = new ServiceManager(services);

    this.containerMetrics = buildContainerMetrics(this.config, properties, applicationName, containerId);

    // Register task factory for the Helix task state model
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();
    taskFactoryMap.put(GOBBLIN_TASK_FACTORY_NAME,
        new GobblinHelixTaskFactory(this.containerMetrics, taskExecutor, taskStateTracker, fs, appWorkDir));
    this.taskStateModelFactory = new TaskStateModelFactory(this.helixManager, taskFactoryMap);
    this.helixManager.getStateMachineEngine().registerStateModelFactory("Task", this.taskStateModelFactory);
  }

  /**
   * Start this {@link GobblinWorkUnitRunner} instance.
   */
  public void start() {
    LOGGER.info(String.format("Starting %s in container %s", this.helixInstanceName, this.containerId));

    // Add a shutdown hook so the task scheduler gets properly shutdown
    addShutdownHook();

    connectHelixManager();

    // Start metric reporting
    if (this.containerMetrics.isPresent()) {
      this.containerMetrics.get()
          .startMetricReportingWithFileSuffix(ConfigUtils.configToState(this.config), this.containerId.toString());
    }

    this.serviceManager.startAsync();
    this.serviceManager.awaitStopped();
  }

  public synchronized void stop() {
    if (this.isStopped || this.stopInProgress) {
      return;
    }

    this.stopInProgress = true;

    LOGGER.info("Stopping the Gobblin Yarn WorkUnit runner");

    // Stop metric reporting
    if (this.containerMetrics.isPresent()) {
      this.containerMetrics.get().stopMetricsReporting();
    }

    try {
      // Give the services 5 minutes to stop to ensure that we are responsive to shutdown requests
      this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.MINUTES);
    } catch (TimeoutException te) {
      LOGGER.error("Timeout in stopping the service manager", te);
    } finally {
      this.taskStateModelFactory.shutdown();

      disconnectHelixManager();
    }

    this.isStopped = true;
  }

  @VisibleForTesting
  boolean isStopped() {
    return this.isStopped;
  }

  @VisibleForTesting
  void connectHelixManager() {
    try {
      this.helixManager.connect();
      this.helixManager.getMessagingService().registerMessageHandlerFactory(Message.MessageType.SHUTDOWN.toString(),
          new ParticipantShutdownMessageHandlerFactory());
      this.helixManager.getMessagingService()
          .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(),
              new ParticipantUserDefinedMessageHandlerFactory());
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }
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
        LOGGER.info("Running the shutdown hook");
        GobblinWorkUnitRunner.this.stop();
      }
    });
  }

  private FileSystem buildFileSystem(Config config, Configuration conf) throws IOException {
    return config.hasPath(ConfigurationKeys.FS_URI_KEY) ?
        FileSystem.get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), conf) :
        FileSystem.get(conf);
  }

  private Optional<ContainerMetrics> buildContainerMetrics(Config config, Properties properties, String applicationName,
      ContainerId containerId) {
    if (GobblinMetrics.isEnabled(properties)) {
      return Optional.of(ContainerMetrics.get(ConfigUtils.configToState(config), applicationName, containerId));
    } else {
      return Optional.absent();
    }
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ParticipantShutdownMessageHandler}s that handle messages
   * of type {@link org.apache.helix.model.Message.MessageType#SHUTDOWN} for shutting down the participants.
   */
  private class ParticipantShutdownMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ParticipantShutdownMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return Message.MessageType.SHUTDOWN.toString();
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
      public HelixTaskResult handleMessage() throws InterruptedException {
        String messageSubType = this._message.getMsgSubType();
        Preconditions
            .checkArgument(messageSubType.equalsIgnoreCase(HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString()),
                String
                    .format("Unknown %s message subtype: %s", Message.MessageType.SHUTDOWN.toString(), messageSubType));

        HelixTaskResult result = new HelixTaskResult();

        if (stopInProgress) {
          result.setSuccess(true);
          return result;
        }

        LOGGER.info("Handling message " + HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString());

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

            HelixProperty helixProperty = helixDataAccessor
                .getProperty(_message.getKey(helixDataAccessor.keyBuilder(), helixManager.getInstanceName()));
            // The absence of the shutdown message indicates it has been removed
            if (helixProperty == null) {
              GobblinWorkUnitRunner.this.stop();
            }
          }
        }, 0, 1, TimeUnit.SECONDS);

        result.setSuccess(true);
        return result;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        LOGGER.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ParticipantUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private class ParticipantUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ParticipantUserDefinedMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return Message.MessageType.USER_DEFINE_MSG.toString();
    }

    @Override
    public void reset() {

    }

    /**
     * A custom {@link MessageHandler} for handling user-defined messages to the participants.
     *
     * <p>
     *   Currently it handles the following sub types of messages:
     *
     *   <ul>
     *     <li>{@link HelixMessageSubTypes#TOKEN_FILE_UPDATED}</li>
     *   </ul>
     * </p>
     */
    private class ParticipantUserDefinedMessageHandler extends MessageHandler {

      public ParticipantUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        String messageSubType = this._message.getMsgSubType();

        if (messageSubType.equalsIgnoreCase(HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString())) {
          LOGGER.info("Handling message " + HelixMessageSubTypes.TOKEN_FILE_UPDATED.toString());

          eventBus.post(new DelegationTokenUpdatedEvent());
          HelixTaskResult helixTaskResult = new HelixTaskResult();
          helixTaskResult.setSuccess(true);
          return helixTaskResult;
        }

        throw new IllegalArgumentException(String
            .format("Unknown %s message subtype: %s", Message.MessageType.USER_DEFINE_MSG.toString(), messageSubType));
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        LOGGER.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinYarnConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Yarn application name");
    options.addOption("i", GobblinYarnConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME, true, "Helix instance name");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinApplicationMaster.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinYarnConfigurationKeys.APPLICATION_NAME_OPTION_NAME) || !cmd
          .hasOption(GobblinYarnConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigurationHelper.updateLog4jConfiguration(GobblinWorkUnitRunner.class,
          Log4jConfigurationHelper.LOG4J_CONFIGURATION_FILE_NAME);

      ContainerId containerId =
          ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));
      String applicationName = cmd.getOptionValue(GobblinYarnConfigurationKeys.APPLICATION_NAME_OPTION_NAME);
      String helixInstanceName = cmd.getOptionValue(GobblinYarnConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME);

      GobblinWorkUnitRunner gobblinWorkUnitRunner =
          new GobblinWorkUnitRunner(applicationName, helixInstanceName, containerId, ConfigFactory.load(),
              Optional.<Path>absent());
      gobblinWorkUnitRunner.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
