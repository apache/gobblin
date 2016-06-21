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

package gobblin.cluster;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.helix.Criteria;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.annotation.Alpha;
import gobblin.cluster.event.ClusterManagerShutdownRequest;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.Tag;
import gobblin.runtime.app.ApplicationException;
import gobblin.runtime.app.ApplicationLauncher;
import gobblin.runtime.app.ServiceBasedAppLauncher;
import gobblin.util.ConfigUtils;
import gobblin.util.logs.Log4jConfigurationHelper;


/**
 * The central cluster manager for Gobblin Clusters.
 *
 * <p>
 *   This class runs the {@link GobblinHelixJobScheduler} for scheduling and running Gobblin jobs.
 *   This class serves as the Helix controller and it uses a {@link HelixManager} to work with Helix.
 * </p>
 *
 * <p>
 *   This class will initiates a graceful shutdown of the cluster in the following conditions:
 *
 *   <ul>
 *     <li>A shutdown request is received via a Helix message of subtype
 *     {@link HelixMessageSubTypes#APPLICATION_MASTER_SHUTDOWN}. Upon receiving such a message,
 *     it will call {@link #stop()} to initiate a graceful shutdown of the cluster</li>
 *     <li>The shutdown hook gets called. The shutdown hook will call {@link #stop()}, which will
 *     start a graceful shutdown of the cluster.</li>
 *   </ul>
 * </p>
 *
 * @author Yinan Li
 */
@Alpha
public class GobblinClusterManager implements ApplicationLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinClusterManager.class);

  private final HelixManager helixManager;

  private volatile boolean stopInProgress = false;

  protected final ServiceBasedAppLauncher applicationLauncher;

  // An EventBus used for communications between services running in the ApplicationMaster
  protected final EventBus eventBus = new EventBus(GobblinClusterManager.class.getSimpleName());

  protected final Path appWorkDir;

  protected final FileSystem fs;

  protected final String applicationId;

  public GobblinClusterManager(String clusterName, String applicationId, Config config) throws Exception {

    // Done to preserve backwards compatibility with the previously hard-coded timeout of 5 minutes
    Properties properties = ConfigUtils.configToProperties(config);
    if (!properties.contains(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS)) {
      properties.setProperty(ServiceBasedAppLauncher.APP_STOP_TIME_SECONDS, Long.toString(300));
    }

    this.applicationId = applicationId;
    this.applicationLauncher = new ServiceBasedAppLauncher(properties, clusterName);

    String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);

    // This will create and register a Helix controller in ZooKeeper
    this.helixManager = buildHelixManager(config, zkConnectionString);

    this.fs = buildFileSystem(config);
    this.appWorkDir = GobblinClusterUtils.getAppWorkDirPath(this.fs, clusterName, applicationId);

    this.applicationLauncher
        .addService(buildGobblinHelixJobScheduler(config, this.appWorkDir, getMetadataTags(clusterName, applicationId)));
    this.applicationLauncher.addService(buildJobConfigurationManager(config));
  }

  /**
   * Start the Gobblin Cluster Manager.
   */
  @Override
  public void start() {
    LOGGER.info("Starting the Gobblin Cluster Manager");

    this.eventBus.register(this);
    connectHelixManager();
    this.applicationLauncher.start();
  }

  /**
   * Stop the Gobblin Cluster Manager.
   */
  @Override
  public synchronized void stop() {
    if (this.stopInProgress) {
      return;
    }

    this.stopInProgress = true;

    LOGGER.info("Stopping the Gobblin Cluster Manager");

    // Send a shutdown request to all GobblinTaskRunners
    sendShutdownRequest();
    try {
      this.applicationLauncher.stop();
    } catch (ApplicationException ae) {
      LOGGER.error("Error while stopping Gobblin Cluster Manager", ae);
    } finally {
      disconnectHelixManager();
    }
  }

  /**
   * Get additional {@link Tag}s required for any type of reporting.
   */
  private List<? extends Tag<?>> getMetadataTags(String applicationName, String applicationId) {
    return Tag.fromMap(
        new ImmutableMap.Builder<String, Object>().put(GobblinClusterMetricTagNames.APPLICATION_NAME, applicationName)
            .put(GobblinClusterMetricTagNames.APPLICATION_ID, applicationId).build());
  }

  /**
   * Build the {@link HelixManager} for the Application Master.
   */
  private HelixManager buildHelixManager(Config config, String zkConnectionString) {
    String helixInstanceName = GobblinClusterManager.class.getSimpleName();
    return HelixManagerFactory.getZKHelixManager(
        config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY), helixInstanceName,
        InstanceType.CONTROLLER, zkConnectionString);
  }

  /**
   * Build the {@link FileSystem} for the Application Master.
   */
  private FileSystem buildFileSystem(Config config)
      throws IOException {
    return config.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem
        .get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), new Configuration())
        : FileSystem.get(new Configuration());
  }

  /**
   * Build the {@link GobblinHelixJobScheduler} for the Application Master.
   */
  private GobblinHelixJobScheduler buildGobblinHelixJobScheduler(Config config, Path appWorkDir,
      List<? extends Tag<?>> metadataTags)
      throws Exception {
    Properties properties = ConfigUtils.configToProperties(config);
    return new GobblinHelixJobScheduler(properties, this.helixManager, this.eventBus, appWorkDir, metadataTags);
  }

  /**
   * Build the {@link JobConfigurationManager} for the Application Master.
   */
  private JobConfigurationManager buildJobConfigurationManager(Config config) {
    Optional<String> jobConfPackagePath =
        config.hasPath(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY) ? Optional
            .of(config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY)) : Optional.<String>absent();
    return new JobConfigurationManager(this.eventBus, jobConfPackagePath);
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleApplicationMasterShutdownRequest(ClusterManagerShutdownRequest shutdownRequest) {
    stop();
  }

  @VisibleForTesting
  EventBus getEventBus() {
    return this.eventBus;
  }

  @VisibleForTesting
  void connectHelixManager() {
    try {
      this.helixManager.connect();
      this.helixManager.addLiveInstanceChangeListener(new GobblinLiveInstanceChangeListener());
      this.helixManager.getMessagingService().registerMessageHandlerFactory(
          Message.MessageType.SHUTDOWN.toString(), new ControllerShutdownMessageHandlerFactory());
      this.helixManager.getMessagingService().registerMessageHandlerFactory(
          Message.MessageType.USER_DEFINE_MSG.toString(), getUserDefinedMessageHandlerFactory());
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
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
    return new ControllerUserDefinedMessageHandlerFactory();
  }

  @VisibleForTesting
  void disconnectHelixManager() {
    if (isHelixManagerConnected()) {
      this.helixManager.disconnect();
    }
  }

  @VisibleForTesting
  boolean isHelixManagerConnected() {
    return this.helixManager.isConnected();
  }

  @VisibleForTesting
  void sendShutdownRequest() {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    criteria.setDataSource(Criteria.DataSource.LIVEINSTANCES);
    criteria.setSessionSpecific(true);

    Message shutdownRequest = new Message(Message.MessageType.SHUTDOWN,
        HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString().toLowerCase() + UUID.randomUUID().toString());
    shutdownRequest.setMsgSubType(HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString());
    shutdownRequest.setMsgState(Message.MessageState.NEW);

    int messagesSent = this.helixManager.getMessagingService().send(criteria, shutdownRequest);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the participants", shutdownRequest.getMsgSubType()));
    }
  }

  @Override
  public void close() throws IOException {
    this.applicationLauncher.close();
  }

  /**
   * A custom implementation of {@link LiveInstanceChangeListener}.
   */
  private static class GobblinLiveInstanceChangeListener implements LiveInstanceChangeListener {

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      for (LiveInstance liveInstance : liveInstances) {
        LOGGER.info("Live Helix participant instance: " + liveInstance.getInstanceName());
      }
    }
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link MessageHandler}s that handle messages of type
   * {@link org.apache.helix.model.Message.MessageType#SHUTDOWN} for shutting down the controller.
   */
  private class ControllerShutdownMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ControllerShutdownMessageHandler(message, context);
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
     * {@link HelixMessageSubTypes#APPLICATION_MASTER_SHUTDOWN}.
     */
    private class ControllerShutdownMessageHandler extends MessageHandler {

      public ControllerShutdownMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        String messageSubType = this._message.getMsgSubType();
        Preconditions.checkArgument(
            messageSubType.equalsIgnoreCase(HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString()),
            String.format("Unknown %s message subtype: %s", Message.MessageType.SHUTDOWN.toString(), messageSubType));

        HelixTaskResult result = new HelixTaskResult();

        if (stopInProgress) {
          result.setSuccess(true);
          return result;
        }

        LOGGER.info("Handling message " + HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());

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
              eventBus.post(new ClusterManagerShutdownRequest());
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
   * A custom {@link MessageHandlerFactory} for {@link ControllerUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  private class ControllerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ControllerUserDefinedMessageHandler(message, context);
    }

    @Override
    public String getMessageType() {
      return Message.MessageType.USER_DEFINE_MSG.toString();
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
    private class ControllerUserDefinedMessageHandler extends MessageHandler {

      public ControllerUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        LOGGER.warn(String
            .format("No handling setup for %s message of subtype: %s", Message.MessageType.USER_DEFINE_MSG.toString(),
                this._message.getMsgSubType()));

        HelixTaskResult helixTaskResult = new HelixTaskResult();
        helixTaskResult.setSuccess(true);
        return helixTaskResult;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        LOGGER.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }


  /**
   * TODO for now the cluster id is hardcoded to 1 both here and in the {@link GobblinTaskRunner}. In the future, the
   * cluster id should be created by the {@link GobblinClusterManager} and passed to each {@link GobblinTaskRunner} via
   * Helix (at least that would be the easiest approach, there are certainly others ways to do it).
   */
  private static String getApplicationId() {
    return "1";
  }

  private static Options buildOptions() {
    Options options = new Options();
    options.addOption("a", GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME, true, "Gobblin application name");
    return options;
  }

  private static void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(GobblinClusterManager.class.getSimpleName(), options);
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    try {
      CommandLine cmd = new DefaultParser().parse(options, args);
      if (!cmd.hasOption(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigurationHelper.updateLog4jConfiguration(GobblinClusterManager.class,
          GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_LOG4J_CONFIGURATION_FILE,
          GobblinClusterConfigurationKeys.GOBBLIN_CLUSTER_LOG4J_CONFIGURATION_FILE);

      try (GobblinClusterManager gobblinClusterManager = new GobblinClusterManager(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME), getApplicationId(),
          ConfigFactory.load())) {

        gobblinClusterManager.start();
      }
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
