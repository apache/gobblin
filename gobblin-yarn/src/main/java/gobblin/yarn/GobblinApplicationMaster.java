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

import java.net.URI;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.ConverterUtils;
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

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.yarn.event.ApplicationMasterShutdownRequest;
import gobblin.yarn.event.DelegationTokenUpdatedEvent;


/**
 * The Yarn ApplicationMaster class for Gobblin.
 *
 * <p>
 *   This class runs the {@link GobblinHelixJobScheduler} for scheduling and running Gobblin jobs,
 *   and the {@link YarnService} for all Yarn-related stuffs like ApplicationMaster registration
 *   and un-registration and Yarn container provisioning. This class serves as the Helix controller
 *   and it uses a {@link HelixManager} to work with Helix.
 * </p>
 *
 * @author ynli
 */
public class GobblinApplicationMaster {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinApplicationMaster.class);

  private final ServiceManager serviceManager;

  private final EventBus eventBus;

  private final HelixManager helixManager;

  private final MetricContext metricContext;

  private final JmxReporter jmxReporter;

  private volatile boolean stopInProgress = false;

  public GobblinApplicationMaster(String applicationName, Config config) throws Exception {
    // An EventBus used for communications between services running in the ApplicationMaster
    this.eventBus = new EventBus(GobblinApplicationMaster.class.getSimpleName());
    this.eventBus.register(this);

    ContainerId containerId =
        ConverterUtils.toContainerId(System.getenv().get(ApplicationConstants.Environment.CONTAINER_ID.key()));
    ApplicationAttemptId applicationAttemptId = containerId.getApplicationAttemptId();

    String zkConnectionString = config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);

    String helixInstanceName = YarnHelixUtils.getHelixInstanceName(YarnHelixUtils.getHostname(), containerId);
    // This will create and register a Helix controller in ZooKeeper
    this.helixManager = HelixManagerFactory.getZKHelixManager(
        config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY), helixInstanceName,
        InstanceType.CONTROLLER, zkConnectionString);

    FileSystem fs = config.hasPath(ConfigurationKeys.FS_URI_KEY) ?
        FileSystem.get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), new Configuration()) :
        FileSystem.get(new Configuration());
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(fs, applicationName, applicationAttemptId.getApplicationId());

    List<Service> services = Lists.newArrayList();
    if (UserGroupInformation.isSecurityEnabled()) {
      LOGGER.info("Adding YarnContainerSecurityManager since security is enabled");
      services.add(new YarnContainerSecurityManager(config, fs, this.eventBus));
    }
    services.add(
        new YarnService(config, applicationName, applicationAttemptId.getApplicationId(), fs, this.eventBus,
            Strings.nullToEmpty(config.getString(GobblinYarnConfigurationKeys.CONTAINER_JVM_ARGS_KEY))));
    services.add(
        new GobblinHelixJobScheduler(YarnHelixUtils.configToProperties(config), this.helixManager, this.eventBus,
            appWorkDir));
    services.add(new JobConfigurationManager(this.eventBus,
        config.hasPath(GobblinYarnConfigurationKeys.JOB_CONF_PACKAGE_PATH_KEY) ? Optional
            .of(config.getString(GobblinYarnConfigurationKeys.JOB_CONF_PACKAGE_PATH_KEY)) : Optional.<String>absent()));

    this.serviceManager = new ServiceManager(services);

    List<Tag<?>> tags = ImmutableList.<Tag<?>>builder()
        .add(new Tag<String>(GobblinYarnMetricTagNames.YARN_APPLICATION_NAME, applicationName))
        .add(new Tag<String>(GobblinYarnMetricTagNames.YARN_APPLICATION_ID,
            applicationAttemptId.getApplicationId().toString()))
        .add(new Tag<String>(GobblinYarnMetricTagNames.CONTAINER_ID, containerId.toString()))
        .add(new Tag<String>(GobblinYarnMetricTagNames.HELIX_INSTANCE_NAME, helixInstanceName))
        .build();
    this.metricContext = MetricContext.builder(GobblinApplicationMaster.class.getSimpleName())
        .addTags(tags)
        .build();

    this.jmxReporter = JmxReporter.forRegistry(this.metricContext)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
  }

  /**
   * Start the ApplicationMaster.
   */
  public void start() {
    LOGGER.info("Starting the Gobblin Yarn ApplicationMaster");

    // Add a shutdown hook so the task scheduler gets properly shutdown
    addShutdownHook();

    try {
      this.helixManager.connect();
      this.helixManager.addLiveInstanceChangeListener(new GobblinLiveInstanceChangeListener());
      this.helixManager.getMessagingService().registerMessageHandlerFactory(
          Message.MessageType.SHUTDOWN.toString(), new ControllerShutdownMessageHandlerFactory());
      this.helixManager.getMessagingService().registerMessageHandlerFactory(
          Message.MessageType.USER_DEFINE_MSG.toString(), new ControllerUserDefinedMessageHandlerFactory()
      );
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }

    // Register JVM metrics to collect and report
    registerJvmMetrics();

    // Start metric reporting
    this.jmxReporter.start();

    // Start all the services running in the ApplicationMaster
    this.serviceManager.startAsync();
    this.serviceManager.awaitHealthy();
  }

  /**
   * Stop the ApplicationMaster.
   */
  public synchronized void stop() {
    if (this.stopInProgress) {
      return;
    }

    this.stopInProgress = true;

    LOGGER.info("Stopping the Gobblin Yarn ApplicationMaster");

    // Send a shutdown request to the containers as a second guard in case Yarn could not stop the containers
    sendShutdownRequest();

    try {
      // Give the services 5 minutes to stop to ensure that we are responsive to shutdown requests
      this.serviceManager.stopAsync().awaitStopped(5, TimeUnit.MINUTES);
    } catch (TimeoutException te) {
      LOGGER.error("Timeout in stopping the service manager", te);
    } finally {
      // Stop metric reporting
      this.jmxReporter.stop();

      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }
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
    this.metricContext.register("jvm.fileDescriptorRatio", new FileDescriptorRatioGauge());
  }

  private void registerMetricSetWithPrefix(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      this.metricContext.register(MetricRegistry.name(prefix, entry.getKey()), entry.getValue());
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

  private void sendShutdownRequest() {
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
              eventBus.post(new ApplicationMasterShutdownRequest());
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
     *   Currently it handles the following sub types of messages:
     *
     *   <ul>
     *     <li>{@link HelixMessageSubTypes#TOKEN_FILE_UPDATED}</li>
     *   </ul>
     * </p>
     */
    private class ControllerUserDefinedMessageHandler extends MessageHandler {

      public ControllerUserDefinedMessageHandler(Message message, NotificationContext context) {
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

        throw new IllegalArgumentException(String.format("Unknown %s message subtype: %s",
            Message.MessageType.USER_DEFINE_MSG.toString(), messageSubType));
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
      if (!cmd.hasOption(GobblinYarnConfigurationKeys.APPLICATION_NAME_OPTION_NAME)) {
        printUsage(options);
        System.exit(1);
      }

      Log4jConfigurationHelper.updateLog4jConfiguration(
          GobblinApplicationMaster.class, Log4jConfigurationHelper.LOG4J_CONFIGURATION_FILE_NAME);

      GobblinApplicationMaster applicationMaster = new GobblinApplicationMaster(
          cmd.getOptionValue(GobblinYarnConfigurationKeys.APPLICATION_NAME_OPTION_NAME), ConfigFactory.load());
      applicationMaster.start();
    } catch (ParseException pe) {
      printUsage(options);
      System.exit(1);
    }
  }
}
