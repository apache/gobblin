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
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Service;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.app.ApplicationException;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


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
@Slf4j
public class GobblinClusterManager implements ApplicationLauncher, StandardMetricsBridge, LeadershipChangeAwareComponent {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinClusterManager.class);

  private StopStatus stopStatus = new StopStatus(false);

  protected ServiceBasedAppLauncher applicationLauncher;

  // An EventBus used for communications between services running in the ApplicationMaster
  protected final EventBus eventBus = new EventBus(GobblinClusterManager.class.getSimpleName());

  protected final Path appWorkDir;

  protected final FileSystem fs;

  protected final String applicationId;

  // thread used to keep process up for an idle controller
  private Thread idleProcessThread;

  // set to true to stop the idle process thread
  private volatile boolean stopIdleProcessThread = false;

  private final boolean isStandaloneMode;

  @Getter
  protected GobblinHelixMultiManager multiManager;
  @Getter
  private MutableJobCatalog jobCatalog;
  @Getter
  private GobblinHelixJobScheduler jobScheduler;
  @Getter
  private JobConfigurationManager jobConfigurationManager;

  private final String clusterName;
  private final Config config;

  public GobblinClusterManager(String clusterName, String applicationId, Config config,
      Optional<Path> appWorkDirOptional) throws Exception {
    this.clusterName = clusterName;
    this.config = config;
    this.isStandaloneMode = ConfigUtils.getBoolean(config, GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE_KEY,
        GobblinClusterConfigurationKeys.DEFAULT_STANDALONE_CLUSTER_MODE);

    this.applicationId = applicationId;

    initializeHelixManager();

    this.fs = buildFileSystem(config);
    this.appWorkDir = appWorkDirOptional.isPresent() ? appWorkDirOptional.get()
        : GobblinClusterUtils.getAppWorkDirPathFromConfig(config, this.fs, clusterName, applicationId);

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
    this.applicationLauncher = new ServiceBasedAppLauncher(properties, this.clusterName);

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
    this.jobScheduler = buildGobblinHelixJobScheduler(config, this.appWorkDir, getMetadataTags(clusterName, applicationId),
        schedulerService);
    this.applicationLauncher.addService(this.jobScheduler);
    this.jobConfigurationManager = buildJobConfigurationManager(config);
    this.applicationLauncher.addService(this.jobConfigurationManager);
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
      this.applicationLauncher.stop();
    } catch (ApplicationException ae) {
      LOGGER.error("Error while stopping Gobblin Cluster application launcher", ae);
    }

    if (this.jobCatalog instanceof Service) {
      ((Service) this.jobCatalog).stopAsync().awaitTerminated();
    }
  }

  /**
   * Start the Gobblin Cluster Manager.
   */
  @Override
  public synchronized void start() {
    LOGGER.info("Starting the Gobblin Cluster Manager");

    this.eventBus.register(this);
    this.multiManager.connect();

    if (this.isStandaloneMode) {
      // standalone mode starts non-daemon threads later, so need to have this thread to keep process up
      this.idleProcessThread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (!GobblinClusterManager.this.stopStatus.isStopInProgress() && !GobblinClusterManager.this.stopIdleProcessThread) {
            try {
              Thread.sleep(300);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      });

      this.idleProcessThread.start();

      // Need this in case a kill is issued to the process so that the idle thread does not keep the process up
      // since GobblinClusterManager.stop() is not called this case.
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          GobblinClusterManager.this.stopIdleProcessThread = true;
        }
      });
    } else {
      startAppLauncherAndServices();
    }
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

    LOGGER.info("Stopping the Gobblin Cluster Manager");

    if (this.idleProcessThread != null) {
      try {
        this.idleProcessThread.join();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }

    // Send a shutdown request to all GobblinTaskRunners unless running in standalone mode.
    // In standalone mode a failing manager should not stop the whole cluster.
    if (!this.isStandaloneMode) {
      sendShutdownRequest();
    }

    stopAppLauncherAndServices();

    this.multiManager.disconnect();
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
   * Build the {@link FileSystem} for the Application Master.
   */
  private FileSystem buildFileSystem(Config config) throws IOException {
    return config.hasPath(ConfigurationKeys.FS_URI_KEY) ? FileSystem
        .get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), new Configuration())
        : FileSystem.get(new Configuration());
  }

  /**
   * Build the {@link GobblinHelixJobScheduler} for the Application Master.
   */
  private GobblinHelixJobScheduler buildGobblinHelixJobScheduler(Config config, Path appWorkDir,
      List<? extends Tag<?>> metadataTags, SchedulerService schedulerService) throws Exception {
    Properties properties = ConfigUtils.configToProperties(config);
    return new GobblinHelixJobScheduler(properties,
        this.multiManager.getJobClusterHelixManager(),
        this.multiManager.getTaskDriverHelixManager(),
        this.eventBus,
        appWorkDir,
        metadataTags,
        schedulerService,
        this.jobCatalog);
  }

  /**
   * Build the {@link JobConfigurationManager} for the Application Master.
   */
  private JobConfigurationManager buildJobConfigurationManager(Config config) {
    return create(config);
  }

  private JobConfigurationManager create(Config config) {
    try {
      if (config.hasPath(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY)) {
        return (JobConfigurationManager) GobblinConstructorUtils.invokeFirstConstructor(Class.forName(
            config.getString(GobblinClusterConfigurationKeys.JOB_CONFIGURATION_MANAGER_KEY)),
            ImmutableList.<Object>of(this.eventBus, config, this.jobCatalog),
            ImmutableList.<Object>of(this.eventBus, config));
      } else {
        return new JobConfigurationManager(this.eventBus, config);
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException |
        ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
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


  /**
   * Creates and returns a {@link MessageHandlerFactory} for handling of Helix
   * {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}s.
   *
   * @returns a {@link MessageHandlerFactory}.
   */
  protected MessageHandlerFactory getUserDefinedMessageHandlerFactory() {
    return new GobblinHelixMultiManager.ControllerUserDefinedMessageHandlerFactory();
  }

  @VisibleForTesting
  void connectHelixManager() {
    this.multiManager.connect();
  }

  @VisibleForTesting
  void disconnectHelixManager() {
    this.multiManager.disconnect();
  }

  @VisibleForTesting
  boolean isHelixManagerConnected() {
    return this.multiManager.isConnected();
  }

  /**
   * In separate controller mode, one controller will manage manager's HA, the other will handle the job dispatching and
   * work unit assignment.
   */
  @VisibleForTesting
  void initializeHelixManager() {
    this.multiManager = new GobblinHelixMultiManager(
        this.config, new Function<Void, MessageHandlerFactory>() {
          @Override
          public MessageHandlerFactory apply(Void aVoid) {
            return GobblinClusterManager.this.getUserDefinedMessageHandlerFactory();
          }
        }, this.eventBus, stopStatus) ;
    this.multiManager.addLeadershipChangeAwareComponent(this);
  }

  @VisibleForTesting
  void sendShutdownRequest() {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
    // #HELIX-0.6.7-WORKAROUND
    // Add this back when messaging to instances is ported to 0.6 branch
    //criteria.setDataSource(Criteria.DataSource.LIVEINSTANCES);
    criteria.setSessionSpecific(true);

    Message shutdownRequest = new Message(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
        HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString().toLowerCase() + UUID.randomUUID().toString());
    shutdownRequest.setMsgSubType(HelixMessageSubTypes.WORK_UNIT_RUNNER_SHUTDOWN.toString());
    shutdownRequest.setMsgState(Message.MessageState.NEW);

    // Wait for 5 minutes
    final int timeout = 300000;

    // #HELIX-0.6.7-WORKAROUND
    // Temporarily bypass the default messaging service to allow upgrade to 0.6.7 which is missing support
    // for messaging to instances
    //int messagesSent = this.helixManager.getMessagingService().send(criteria, shutdownRequest,
    //    new NoopReplyHandler(), timeout);
    GobblinHelixMessagingService messagingService = new GobblinHelixMessagingService(this.multiManager.getJobClusterHelixManager());

    int messagesSent = messagingService.send(criteria, shutdownRequest,
            new NoopReplyHandler(), timeout);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the participants", shutdownRequest.getMsgSubType()));
    }
  }

  @Override
  public void close() throws IOException {
    this.applicationLauncher.close();
  }

  @Override
  public Collection<StandardMetrics> getStandardMetricsCollection() {
    List<StandardMetrics> list = new ArrayList();
    list.addAll(this.jobScheduler.getStandardMetricsCollection());
    list.addAll(this.multiManager.getStandardMetricsCollection());
    list.addAll(this.jobCatalog.getStandardMetricsCollection());
    list.addAll(this.jobConfigurationManager.getStandardMetricsCollection());
    return list;
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
    options.addOption("s", GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE, true, "Standalone cluster mode");
    options.addOption("i", GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME, true, "Helix instance name");
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

      boolean isStandaloneClusterManager = false;
      if (cmd.hasOption(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE)) {
        isStandaloneClusterManager = Boolean.parseBoolean(cmd.getOptionValue(GobblinClusterConfigurationKeys.STANDALONE_CLUSTER_MODE, "false"));
      }

      LOGGER.info(JvmUtils.getJvmInputArguments());
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

      try (GobblinClusterManager gobblinClusterManager = new GobblinClusterManager(
          cmd.getOptionValue(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME), getApplicationId(),
          config, Optional.<Path>absent())) {

        // In AWS / Yarn mode, the cluster Launcher takes care of setting up Helix cluster
        /// .. but for Standalone mode, we go via this main() method, so setup the cluster here
        if (isStandaloneClusterManager) {
          // Create Helix cluster and connect to it
          String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
          String helixClusterName = config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
          HelixUtils.createGobblinHelixCluster(zkConnectionString, helixClusterName, false);
          LOGGER.info("Created Helix cluster " + helixClusterName);
        }

        gobblinClusterManager.start();
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
