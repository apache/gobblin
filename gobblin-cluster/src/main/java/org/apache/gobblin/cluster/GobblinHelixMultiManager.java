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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.helix.ControllerChangeListener;
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
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.WorkflowConfig;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import com.typesafe.config.Config;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareHistogram;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.ConfigUtils;

/**
 * Encapsulate all Helix related components: controller, participants, etc.
 * Provide all kinds of callbacks, listeners, message handlers that each Helix components need to register.
 */
@Slf4j
public class GobblinHelixMultiManager implements StandardMetricsBridge {

  /**
   * Helix manager to handle cluster manager leader election.
   * Corresponds to cluster with key name {@link GobblinClusterConfigurationKeys#MANAGER_CLUSTER_NAME_KEY} iff dedicatedManagerCluster is true.
   * Corresponds to cluster with key name {@link GobblinClusterConfigurationKeys#HELIX_CLUSTER_NAME_KEY} iff dedicatedManagerCluster is false.
   */
  @Getter
  private HelixManager managerClusterHelixManager = null;

  /**
   * Helix manager to handle job distribution.
   * Corresponds to cluster with key name {@link GobblinClusterConfigurationKeys#HELIX_CLUSTER_NAME_KEY}.
   */
  @Getter
  private HelixManager jobClusterHelixManager = null;

  /**
   * Helix manager to handle planning job distribution.
   * Corresponds to cluster with key name {@link GobblinClusterConfigurationKeys#HELIX_CLUSTER_NAME_KEY}.
   */
  @Getter
  private Optional<HelixManager> taskDriverHelixManager = Optional.empty();

  /**
   * Helix controller for job distribution. Effective only iff below two conditions are established:
   * 1. In {@link GobblinHelixMultiManager#dedicatedManagerCluster} mode.
   * 2. {@link GobblinHelixMultiManager#dedicatedJobClusterController} is turned on.
   * Typically used for unit test and local deployment.
   */
  private Optional<HelixManager> jobClusterController = Optional.empty();

  /**
   * Helix controller for planning job distribution. Effective only iff below two conditions are established:
   * 1. In {@link GobblinHelixMultiManager#dedicatedManagerCluster} mode.
   * 2. {@link GobblinHelixMultiManager#dedicatedTaskDriverCluster} is turned on.
   * Typically used for unit test and local deployment.
   */
  private Optional<HelixManager> taskDriverClusterController = Optional.empty();

  /**
   * Separate manager cluster and job distribution cluster iff this flag is turned on. Otherwise {@link GobblinHelixMultiManager#jobClusterHelixManager}
   * is same as {@link GobblinHelixMultiManager#managerClusterHelixManager}.
   */
  private boolean dedicatedManagerCluster = false;

  private boolean dedicatedTaskDriverCluster = false;

  /**
   * Create a dedicated controller for job distribution.
   */
  private boolean dedicatedJobClusterController = true;

  /**
   * Create a dedicated controller for planning job distribution.
   */
  private boolean dedicatedTaskDriverClusterController = true;

  @Getter
  boolean isLeader = false;
  boolean isStandaloneMode = false;
  private GobblinClusterManager.StopStatus stopStatus;
  private Config config;
  private EventBus eventBus;
  private final MetricContext metricContext;
  private final HelixManagerMetrics metrics;
  private final MessageHandlerFactory userDefinedMessageHandlerFactory;
  private List<LeadershipChangeAwareComponent> leadershipChangeAwareComponents = Lists.newArrayList();

  public GobblinHelixMultiManager(
      Config config,
      Function<Void, MessageHandlerFactory> messageHandlerFactoryFunction,
      EventBus eventBus,
      GobblinClusterManager.StopStatus stopStatus) {
    this.config = config;
    this.eventBus = eventBus;
    this.stopStatus = stopStatus;
    this.isStandaloneMode = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE_KEY,
        GobblinClusterConfigurationKeys.DEFAULT_STANDALONE_CLUSTER_MODE);
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), this.getClass());
    this.metrics = new HelixManagerMetrics(this.metricContext, this.config);
    this.dedicatedManagerCluster = ConfigUtils.getBoolean(config,
       GobblinClusterConfigurationKeys.DEDICATED_MANAGER_CLUSTER_ENABLED,false);
    this.dedicatedTaskDriverCluster = ConfigUtils.getBoolean(config,
        GobblinClusterConfigurationKeys.DEDICATED_TASK_DRIVER_CLUSTER_ENABLED, false);
    this.userDefinedMessageHandlerFactory = messageHandlerFactoryFunction.apply(null);
    initialize();
  }

  protected void addLeadershipChangeAwareComponent (LeadershipChangeAwareComponent component) {
    this.leadershipChangeAwareComponents.add(component);
  }

  /**
   * Build the {@link HelixManager} for the Application Master.
   */
  protected static HelixManager buildHelixManager(Config config, String zkConnectionString, String clusterName, InstanceType type) {
    String helixInstanceName = ConfigUtils.getString(config, GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_KEY,
        GobblinClusterManager.class.getSimpleName());
    return HelixManagerFactory.getZKHelixManager(
        config.getString(clusterName), helixInstanceName, type, zkConnectionString);
  }

  public void initialize() {
    Preconditions.checkArgument(this.config.hasPath(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY));
    String zkConnectionString = this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    log.info("Using ZooKeeper connection string: " + zkConnectionString);

    if (this.dedicatedManagerCluster) {
      Preconditions.checkArgument(this.config.hasPath(GobblinClusterConfigurationKeys.MANAGER_CLUSTER_NAME_KEY));
      log.info("We will use separate clusters to manage GobblinClusterManager and job distribution.");
      // This will create and register a Helix controller in ZooKeeper
      this.managerClusterHelixManager = buildHelixManager(this.config,
          zkConnectionString,
          GobblinClusterConfigurationKeys.MANAGER_CLUSTER_NAME_KEY,
          InstanceType.CONTROLLER);

      // This will create a Helix administrator to dispatch jobs to ZooKeeper
      this.jobClusterHelixManager = buildHelixManager(this.config,
          zkConnectionString,
          GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY,
          InstanceType.ADMINISTRATOR);

      // This will create a dedicated controller for job distribution
      this.dedicatedJobClusterController = ConfigUtils.getBoolean(
          this.config,
          GobblinClusterConfigurationKeys.DEDICATED_JOB_CLUSTER_CONTROLLER_ENABLED,
          true);

      if (this.dedicatedJobClusterController) {
        this.jobClusterController = Optional.of(GobblinHelixMultiManager
            .buildHelixManager(this.config, zkConnectionString, GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY,
                InstanceType.CONTROLLER));
      }

      // This will creat a dedicated controller for planning job distribution
      if (this.dedicatedTaskDriverCluster) {
        // This will create a Helix administrator to dispatch jobs to ZooKeeper
        this.taskDriverHelixManager = Optional.of(buildHelixManager(this.config,
            zkConnectionString,
            GobblinClusterConfigurationKeys.TASK_DRIVER_CLUSTER_NAME_KEY,
            InstanceType.ADMINISTRATOR));

        this.dedicatedTaskDriverClusterController = ConfigUtils.getBoolean(
            this.config,
            GobblinClusterConfigurationKeys.DEDICATED_TASK_DRIVER_CLUSTER_CONTROLLER_ENABLED,
            true);

        if (this.dedicatedTaskDriverClusterController) {
          this.taskDriverClusterController = Optional.of(GobblinHelixMultiManager
              .buildHelixManager(this.config, zkConnectionString, GobblinClusterConfigurationKeys.TASK_DRIVER_CLUSTER_NAME_KEY,
                  InstanceType.CONTROLLER));
        }
      }
    } else {
      log.info("We will use same cluster to manage GobblinClusterManager and job distribution.");
      // This will create and register a Helix controller in ZooKeeper
      this.managerClusterHelixManager = buildHelixManager(this.config,
          zkConnectionString,
          GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY,
          InstanceType.CONTROLLER);
      this.jobClusterHelixManager = this.managerClusterHelixManager;
    }
  }

  @VisibleForTesting
  protected void connect() {
    try {
      this.isLeader = false;
      this.managerClusterHelixManager.connect();
      if (this.dedicatedManagerCluster) {
        if (jobClusterController.isPresent()) {
          this.jobClusterController.get().connect();
        }
        if (this.dedicatedTaskDriverCluster) {
          if (taskDriverClusterController.isPresent()) {
            this.taskDriverClusterController.get().connect();
          }
        }
        this.jobClusterHelixManager.connect();
        if (this.taskDriverHelixManager.isPresent()) {
          this.taskDriverHelixManager.get().connect();
        }
      }

      this.jobClusterHelixManager.addLiveInstanceChangeListener(new GobblinLiveInstanceChangeListener());
      this.jobClusterHelixManager.getMessagingService()
          .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), userDefinedMessageHandlerFactory);

      this.jobClusterHelixManager.getMessagingService()
          .registerMessageHandlerFactory(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE, new ControllerShutdownMessageHandlerFactory());
      // standalone mode listens for controller change
      if (this.isStandaloneMode) {
        // Subscribe to leadership changes
        this.managerClusterHelixManager.addControllerListener(new ControllerChangeListener() {
          @Override
          public void onControllerChange(NotificationContext changeContext) {
            handleLeadershipChange(changeContext);
          }
        });
      }
    } catch (Exception e) {
      log.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }
  }

  protected boolean isConnected() {
    return managerClusterHelixManager.isConnected() && jobClusterHelixManager.isConnected();
  }

  protected void disconnect() {
    if (managerClusterHelixManager.isConnected()) {
      this.managerClusterHelixManager.disconnect();
    }

    if (this.dedicatedManagerCluster) {
      if (jobClusterHelixManager.isConnected()) {
        this.jobClusterHelixManager.disconnect();
      }

      if (taskDriverHelixManager.isPresent()) {
        this.taskDriverHelixManager.get().disconnect();
      }

      if (jobClusterController.isPresent() && jobClusterController.get().isConnected()) {
        this.jobClusterController.get().disconnect();
      }

      if (taskDriverClusterController.isPresent() && taskDriverClusterController.get().isConnected()) {
        this.taskDriverClusterController.get().disconnect();
      }
    }
  }

  /**
   * A custom implementation of {@link LiveInstanceChangeListener}.
   */
  private static class GobblinLiveInstanceChangeListener implements LiveInstanceChangeListener {

    @Override
    public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
      for (LiveInstance liveInstance : liveInstances) {
        log.info("Live Helix participant instance: " + liveInstance.getInstanceName());
      }
    }
  }

  /**
   * Handle leadership change.
   * The applicationLauncher is only started on the leader.
   * The leader cleans up existing jobs before starting the applicationLauncher.
   * @param changeContext notification context
   */
  @VisibleForTesting
  void handleLeadershipChange(NotificationContext changeContext) {
    this.metrics.clusterLeadershipChange.update(1);
    if (this.managerClusterHelixManager.isLeader()) {
      // can get multiple notifications on a leadership change,
      // so only start the application launcher the first time
      // the notification is received
      log.info("Leader notification for {} isLeader {} HM.isLeader {}",
          managerClusterHelixManager.getInstanceName(),
          isLeader,
          managerClusterHelixManager.isLeader());

      if (!isLeader) {
        log.info("New Helix Controller leader {}", this.managerClusterHelixManager.getInstanceName());

        // Clean up existing jobs
        TaskDriver taskDriver = new TaskDriver(this.jobClusterHelixManager);
        Map<String, WorkflowConfig> workflows = taskDriver.getWorkflows();

        for (Map.Entry<String, WorkflowConfig> entry : workflows.entrySet()) {
          String queueName = entry.getKey();
          WorkflowConfig workflowConfig = entry.getValue();

          // request delete if not already requested
          if (workflowConfig.getTargetState() != TargetState.DELETE) {
            taskDriver.delete(queueName);

            log.info("Requested delete of queue {}", queueName);
          }
        }

        for (LeadershipChangeAwareComponent c: this.leadershipChangeAwareComponents) {
          c.becomeActive();
        }

        isLeader = true;
      }
    } else {
      // stop and reinitialize services since they are not restartable
      // this prepares them to start when this cluster manager becomes a leader
      if (isLeader) {
        isLeader = false;
        for (LeadershipChangeAwareComponent c: this.leadershipChangeAwareComponents) {
          c.becomeStandby();
        }
      }
    }
  }


  /**
   * A custom {@link MessageHandlerFactory} for {@link MessageHandler}s that handle messages of type
   * "SHUTDOWN" for shutting down the controller.
   */
  private class ControllerShutdownMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ControllerShutdownMessageHandler(message, context);
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
            String.format("Unknown %s message subtype: %s", GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE, messageSubType));

        HelixTaskResult result = new HelixTaskResult();

        if (stopStatus.isStopInProgress()) {
          result.setSuccess(true);
          return result;
        }

        log.info("Handling message " + HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());

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
        log.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }

  /**
   * A custom {@link MessageHandlerFactory} for {@link ControllerUserDefinedMessageHandler}s that
   * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
   */
  static class ControllerUserDefinedMessageHandlerFactory implements MessageHandlerFactory {

    @Override
    public MessageHandler createHandler(Message message, NotificationContext context) {
      return new ControllerUserDefinedMessageHandler(message, context);
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
     *   {@link GobblinClusterManager#getUserDefinedMessageHandlerFactory()}.
     * </p>
     */
    private static class ControllerUserDefinedMessageHandler extends MessageHandler {

      public ControllerUserDefinedMessageHandler(Message message, NotificationContext context) {
        super(message, context);
      }

      @Override
      public HelixTaskResult handleMessage() throws InterruptedException {
        log.warn(String
            .format("No handling setup for %s message of subtype: %s", Message.MessageType.USER_DEFINE_MSG.toString(),
                this._message.getMsgSubType()));

        HelixTaskResult helixTaskResult = new HelixTaskResult();
        helixTaskResult.setSuccess(true);
        return helixTaskResult;
      }

      @Override
      public void onError(Exception e, ErrorCode code, ErrorType type) {
        log.error(
            String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
      }
    }
  }


  /**
   * Helix related metrics
   */
  private class HelixManagerMetrics extends StandardMetricsBridge.StandardMetrics {
    public static final String CLUSTER_LEADERSHIP_CHANGE = "clusterLeadershipChange";
    private ContextAwareHistogram clusterLeadershipChange;
    public HelixManagerMetrics(final MetricContext metricContext, final Config config) {
      int timeWindowSizeInMinutes = ConfigUtils.getInt(config, ConfigurationKeys.METRIC_TIMER_WINDOW_SIZE_IN_MINUTES, ConfigurationKeys.DEFAULT_METRIC_TIMER_WINDOW_SIZE_IN_MINUTES);
      this.clusterLeadershipChange = metricContext.contextAwareHistogram(CLUSTER_LEADERSHIP_CHANGE, timeWindowSizeInMinutes, TimeUnit.MINUTES);
      this.contextAwareMetrics.add(clusterLeadershipChange);
    }

    @Override
    public String getName() {
      return GobblinClusterManager.class.getName();
    }
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    return ImmutableList.of(this.metrics);
  }
}
