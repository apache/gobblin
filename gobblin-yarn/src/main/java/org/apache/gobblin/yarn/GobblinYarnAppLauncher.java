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

package org.apache.gobblin.yarn;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.mail.EmailException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.Criteria;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.GobblinHelixConstants;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.rest.JobExecutionInfoServer;
import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.EmailUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.io.StreamUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.logs.LogCopier;
import org.apache.gobblin.yarn.event.ApplicationReportArrivalEvent;
import org.apache.gobblin.yarn.event.GetApplicationReportFailureEvent;


/**
 * A client driver to launch Gobblin as a Yarn application.
 *
 * <p>
 *   This class, upon starting, will check if there's a Yarn application that it has previously submitted and
 *   it is able to reconnect to. More specifically, it checks if an application with the same application name
 *   exists and can be reconnected to, i.e., if the application has not completed yet. If so, it simply starts
 *   monitoring that application.
 * </p>
 *
 * <p>
 *   On the other hand, if there's no such a reconnectable Yarn application, This class will launch a new Yarn
 *   application and start the {@link GobblinApplicationMaster}. It also persists the new application ID so it
 *   is able to reconnect to the Yarn application if it is restarted for some reason. Once the application is
 *   launched, this class starts to monitor the application by periodically polling the status of the application
 *   through a {@link ListeningExecutorService}.
 * </p>
 *
 * <p>
 *   If a shutdown signal is received, it sends a Helix
 *   {@link org.apache.helix.model.Message.MessageType#SCHEDULER_MSG} to the {@link GobblinApplicationMaster}
 *   asking it to shutdown and release all the allocated containers. It also sends an email notification for
 *   the shutdown if {@link GobblinYarnConfigurationKeys#EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY} is {@code true}.
 * </p>
 *
 * <p>
 *   This class has a scheduled task to get the {@link ApplicationReport} of the Yarn application periodically.
 *   Since it may fail to get the {@link ApplicationReport} due to reason such as the Yarn cluster is down for
 *   maintenance, it keeps track of the count of consecutive failures to get the {@link ApplicationReport}. If
 *   this count exceeds the maximum number allowed, it will initiate a shutdown.
 * </p>
 *
 * @author Yinan Li
 */
public class GobblinYarnAppLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinYarnAppLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private static final String GOBBLIN_YARN_APPLICATION_TYPE = "GOBBLIN_YARN";

  // The set of Yarn application types this class is interested in. This is used to
  // lookup the application this class has launched previously upon restarting.
  private static final Set<String> APPLICATION_TYPES = ImmutableSet.of(GOBBLIN_YARN_APPLICATION_TYPE);

  // The set of Yarn application states under which the driver can reconnect to the Yarn application after restart
  private static final EnumSet<YarnApplicationState> RECONNECTABLE_APPLICATION_STATES = EnumSet.of(
      YarnApplicationState.NEW,
      YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED,
      YarnApplicationState.ACCEPTED,
      YarnApplicationState.RUNNING
  );

  private final String applicationName;
  private final String appQueueName;

  private final Config config;

  private final HelixManager helixManager;

  private final Configuration yarnConfiguration;
  private final YarnClient yarnClient;
  private final FileSystem fs;

  private final EventBus eventBus = new EventBus(GobblinYarnAppLauncher.class.getSimpleName());

  private final ScheduledExecutorService applicationStatusMonitor;
  private final long appReportIntervalMinutes;

  private final Optional<String> appMasterJvmArgs;

  private final Path sinkLogRootDir;

  private final Closer closer = Closer.create();

  // Yarn application ID
  private volatile Optional<ApplicationId> applicationId = Optional.absent();

  private volatile Optional<ServiceManager> serviceManager = Optional.absent();

  // Maximum number of consecutive failures allowed to get the ApplicationReport
  private final int maxGetApplicationReportFailures;

  // A count on the number of consecutive failures on getting the ApplicationReport
  private final AtomicInteger getApplicationReportFailureCount = new AtomicInteger();

  // This flag tells if the Yarn application has already completed. This is used to
  // tell if it is necessary to send a shutdown message to the ApplicationMaster.
  private volatile boolean applicationCompleted = false;

  private volatile boolean stopped = false;

  private final boolean emailNotificationOnShutdown;

  public GobblinYarnAppLauncher(Config config, YarnConfiguration yarnConfiguration) throws IOException {
    this.config = config;

    this.applicationName = config.getString(GobblinYarnConfigurationKeys.APPLICATION_NAME_KEY);
    this.appQueueName = config.getString(GobblinYarnConfigurationKeys.APP_QUEUE_KEY);

    String zkConnectionString = config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY), GobblinClusterUtils.getHostname(),
        InstanceType.SPECTATOR, zkConnectionString);

    this.yarnConfiguration = yarnConfiguration;
    this.yarnConfiguration.set("fs.automatic.close", "false");
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConfiguration);

    this.fs = config.hasPath(ConfigurationKeys.FS_URI_KEY) ?
        FileSystem.get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), this.yarnConfiguration) :
        FileSystem.get(this.yarnConfiguration);
    this.closer.register(this.fs);

    this.applicationStatusMonitor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("GobblinYarnAppStatusMonitor")));
    this.appReportIntervalMinutes = config.getLong(GobblinYarnConfigurationKeys.APP_REPORT_INTERVAL_MINUTES_KEY);

    this.appMasterJvmArgs = config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinYarnConfigurationKeys.APP_MASTER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    this.sinkLogRootDir = new Path(config.getString(GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY));

    this.maxGetApplicationReportFailures = config.getInt(GobblinYarnConfigurationKeys.MAX_GET_APP_REPORT_FAILURES_KEY);

    this.emailNotificationOnShutdown =
        config.getBoolean(GobblinYarnConfigurationKeys.EMAIL_NOTIFICATION_ON_SHUTDOWN_KEY);
  }

  /**
   * Launch a new Gobblin instance on Yarn.
   *
   * @throws IOException if there's something wrong launching the application
   * @throws YarnException if there's something wrong launching the application
   */
  public void launch() throws IOException, YarnException {
    this.eventBus.register(this);

    String clusterName = this.config.getString(GobblinClusterConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    HelixUtils.createGobblinHelixCluster(
        this.config.getString(GobblinClusterConfigurationKeys.ZK_CONNECTION_STRING_KEY), clusterName);
    LOGGER.info("Created Helix cluster " + clusterName);

    connectHelixManager();

    startYarnClient();

    this.applicationId = getApplicationId();

    this.applicationStatusMonitor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          eventBus.post(new ApplicationReportArrivalEvent(yarnClient.getApplicationReport(applicationId.get())));
        } catch (YarnException | IOException e) {
          LOGGER.error("Failed to get application report for Gobblin Yarn application " + applicationId.get(), e);
          eventBus.post(new GetApplicationReportFailureEvent(e));
        }
      }
    }, 0, this.appReportIntervalMinutes, TimeUnit.MINUTES);

    List<Service> services = Lists.newArrayList();
    if (this.config.hasPath(GobblinYarnConfigurationKeys.KEYTAB_FILE_PATH)) {
      LOGGER.info("Adding YarnAppSecurityManager since login is keytab based");
      services.add(buildYarnAppSecurityManager());
    }
    if (!this.config.hasPath(GobblinYarnConfigurationKeys.LOG_COPIER_DISABLE_DRIVER_COPY) ||
        !this.config.getBoolean(GobblinYarnConfigurationKeys.LOG_COPIER_DISABLE_DRIVER_COPY)) {
      services.add(buildLogCopier(this.config,
        new Path(this.sinkLogRootDir, this.applicationName + Path.SEPARATOR + this.applicationId.get().toString()),
        GobblinClusterUtils.getAppWorkDirPath(this.fs, this.applicationName, this.applicationId.get().toString())));
    }
    if (config.getBoolean(ConfigurationKeys.JOB_EXECINFO_SERVER_ENABLED_KEY)) {
      LOGGER.info("Starting the job execution info server since it is enabled");
      Properties properties = ConfigUtils.configToProperties(config);
      JobExecutionInfoServer executionInfoServer = new JobExecutionInfoServer(properties);
      services.add(executionInfoServer);
      if (config.getBoolean(ConfigurationKeys.ADMIN_SERVER_ENABLED_KEY)) {
        LOGGER.info("Starting the admin UI server since it is enabled");
        services.add(ServiceBasedAppLauncher.createAdminServer(properties,
                                                               executionInfoServer.getAdvertisedServerUri()));
      }
    } else if (config.getBoolean(ConfigurationKeys.ADMIN_SERVER_ENABLED_KEY)) {
      LOGGER.warn("NOT starting the admin UI because the job execution info server is NOT enabled");
    }

    this.serviceManager = Optional.of(new ServiceManager(services));
    // Start all the services running in the ApplicationMaster
    this.serviceManager.get().startAsync();
  }

  /**
   * Stop this {@link GobblinYarnAppLauncher} instance.
   *
   * @throws IOException if this {@link GobblinYarnAppLauncher} instance fails to clean up its working directory.
   */
  public synchronized void stop() throws IOException, TimeoutException {
    if (this.stopped) {
      return;
    }

    LOGGER.info("Stopping the " + GobblinYarnAppLauncher.class.getSimpleName());

    try {
      if (this.applicationId.isPresent() && !this.applicationCompleted) {
        // Only send the shutdown message if the application has been successfully submitted and is still running
        sendShutdownRequest();
      }

      if (this.serviceManager.isPresent()) {
        this.serviceManager.get().stopAsync().awaitStopped(5, TimeUnit.MINUTES);
      }

      ExecutorsUtils.shutdownExecutorService(this.applicationStatusMonitor, Optional.of(LOGGER), 5, TimeUnit.MINUTES);

      stopYarnClient();

      disconnectHelixManager();
    } finally {
      try {
        if (this.applicationId.isPresent()) {
          cleanUpAppWorkDirectory(this.applicationId.get());
        }
      } finally {
        this.closer.close();
      }
    }

    this.stopped = true;
  }

  @Subscribe
  public void handleApplicationReportArrivalEvent(ApplicationReportArrivalEvent applicationReportArrivalEvent) {
    ApplicationReport applicationReport = applicationReportArrivalEvent.getApplicationReport();

    YarnApplicationState appState = applicationReport.getYarnApplicationState();
    LOGGER.info("Gobblin Yarn application state: " + appState.toString());

    // Reset the count on failures to get the ApplicationReport when there's one success
    this.getApplicationReportFailureCount.set(0);

    if (appState == YarnApplicationState.FINISHED ||
        appState == YarnApplicationState.FAILED ||
        appState == YarnApplicationState.KILLED) {

      applicationCompleted = true;

      LOGGER.info("Gobblin Yarn application finished with final status: " +
          applicationReport.getFinalApplicationStatus().toString());
      if (applicationReport.getFinalApplicationStatus() == FinalApplicationStatus.FAILED) {
        LOGGER.error("Gobblin Yarn application failed for the following reason: " + applicationReport.getDiagnostics());
      }

      try {
        GobblinYarnAppLauncher.this.stop();
      } catch (IOException ioe) {
        LOGGER.error("Failed to close the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
      } catch (TimeoutException te) {
        LOGGER.error("Timeout in stopping the service manager", te);
      } finally {
        if (this.emailNotificationOnShutdown) {
          sendEmailOnShutdown(Optional.of(applicationReport));
        }
      }
    }
  }

  @Subscribe
  public void handleGetApplicationReportFailureEvent(
      GetApplicationReportFailureEvent getApplicationReportFailureEvent) {
    int numConsecutiveFailures = this.getApplicationReportFailureCount.incrementAndGet();
    if (numConsecutiveFailures > this.maxGetApplicationReportFailures) {
      LOGGER.warn(String
          .format("Number of consecutive failures to get the ApplicationReport %d exceeds the threshold %d",
              numConsecutiveFailures, this.maxGetApplicationReportFailures));

      try {
        stop();
      } catch (IOException ioe) {
        LOGGER.error("Failed to close the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
      } catch (TimeoutException te) {
        LOGGER.error("Timeout in stopping the service manager", te);
      } finally {
        if (this.emailNotificationOnShutdown) {
          sendEmailOnShutdown(Optional.<ApplicationReport>absent());
        }
      }
    }
  }

  @VisibleForTesting
  void connectHelixManager() {
    try {
      this.helixManager.connect();
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

  @VisibleForTesting
  void startYarnClient() {
    this.yarnClient.start();
  }

  @VisibleForTesting
  void stopYarnClient() {
    this.yarnClient.stop();
  }

  private Optional<ApplicationId> getApplicationId() throws YarnException, IOException {
    Optional<ApplicationId> reconnectableApplicationId = getReconnectableApplicationId();
    if (reconnectableApplicationId.isPresent()) {
      LOGGER.info("Found reconnectable application with application ID: " + reconnectableApplicationId.get());
      return reconnectableApplicationId;
    }

    LOGGER.info("No reconnectable application found so submitting a new application");
    return Optional.of(setupAndSubmitApplication());
  }

  @VisibleForTesting
  Optional<ApplicationId> getReconnectableApplicationId() throws YarnException, IOException {
    List<ApplicationReport> applicationReports =
        this.yarnClient.getApplications(APPLICATION_TYPES, RECONNECTABLE_APPLICATION_STATES);
    if (applicationReports == null || applicationReports.isEmpty()) {
      return Optional.absent();
    }

    // Try to find an application with a matching application name
    for (ApplicationReport applicationReport : applicationReports) {
      if (this.applicationName.equals(applicationReport.getName())) {
        return Optional.of(applicationReport.getApplicationId());
      }
    }

    return Optional.absent();
  }

  /**
   * Setup and submit the Gobblin Yarn application.
   *
   * @throws IOException if there's anything wrong setting up and submitting the Yarn application
   * @throws YarnException if there's anything wrong setting up and submitting the Yarn application
   */
  @VisibleForTesting
  ApplicationId setupAndSubmitApplication() throws IOException, YarnException {
    YarnClientApplication gobblinYarnApp = this.yarnClient.createApplication();
    ApplicationSubmissionContext appSubmissionContext = gobblinYarnApp.getApplicationSubmissionContext();
    appSubmissionContext.setApplicationType(GOBBLIN_YARN_APPLICATION_TYPE);
    ApplicationId applicationId = appSubmissionContext.getApplicationId();

    GetNewApplicationResponse newApplicationResponse = gobblinYarnApp.getNewApplicationResponse();
    // Set up resource type requirements for ApplicationMaster
    Resource resource = prepareContainerResource(newApplicationResponse);

    // Add lib jars, and jars and files that the ApplicationMaster need as LocalResources
    Map<String, LocalResource> appMasterLocalResources = addAppMasterLocalResources(applicationId);

    ContainerLaunchContext amContainerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    amContainerLaunchContext.setLocalResources(appMasterLocalResources);
    amContainerLaunchContext.setEnvironment(YarnHelixUtils.getEnvironmentVariables(this.yarnConfiguration));
    amContainerLaunchContext.setCommands(Lists.newArrayList(buildApplicationMasterCommand(resource.getMemory())));
    if (UserGroupInformation.isSecurityEnabled()) {
      setupSecurityTokens(amContainerLaunchContext);
    }

    // Setup the application submission context
    appSubmissionContext.setApplicationName(this.applicationName);
    appSubmissionContext.setResource(resource);
    appSubmissionContext.setQueue(this.appQueueName);
    appSubmissionContext.setPriority(Priority.newInstance(0));
    appSubmissionContext.setAMContainerSpec(amContainerLaunchContext);

    // Also setup container local resources by copying local jars and files the container need to HDFS
    addContainerLocalResources(applicationId);

    // Submit the application
    LOGGER.info("Submitting application " + applicationId);
    this.yarnClient.submitApplication(appSubmissionContext);

    LOGGER.info("Application successfully submitted and accepted");
    ApplicationReport applicationReport = this.yarnClient.getApplicationReport(applicationId);
    LOGGER.info("Application Name: " + applicationReport.getName());
    LOGGER.info("Application Tracking URL: " + applicationReport.getTrackingUrl());
    LOGGER.info("Application User: " + applicationReport.getUser() + " Queue: " + applicationReport.getQueue());

    return applicationId;
  }

  private Resource prepareContainerResource(GetNewApplicationResponse newApplicationResponse) {
    int memoryMbs = this.config.getInt(GobblinYarnConfigurationKeys.APP_MASTER_MEMORY_MBS_KEY);
    int maximumMemoryCapacity = newApplicationResponse.getMaximumResourceCapability().getMemory();
    if (memoryMbs > maximumMemoryCapacity) {
      LOGGER.info(String.format("Specified AM memory [%d] is above the maximum memory capacity [%d] of the "
          + "cluster, using the maximum memory capacity instead.", memoryMbs, maximumMemoryCapacity));
      memoryMbs = maximumMemoryCapacity;
    }

    int vCores = this.config.getInt(GobblinYarnConfigurationKeys.APP_MASTER_CORES_KEY);
    int maximumVirtualCoreCapacity = newApplicationResponse.getMaximumResourceCapability().getVirtualCores();
    if (vCores > maximumVirtualCoreCapacity) {
      LOGGER.info(String.format("Specified AM vcores [%d] is above the maximum vcore capacity [%d] of the "
          + "cluster, using the maximum vcore capacity instead.", memoryMbs, maximumMemoryCapacity));
      vCores = maximumVirtualCoreCapacity;
    }

    // Set up resource type requirements for ApplicationMaster
    return Resource.newInstance(memoryMbs, vCores);
  }

  private Map<String, LocalResource> addAppMasterLocalResources(ApplicationId applicationId) throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPath(this.fs, this.applicationName, applicationId.toString());
    Path appMasterWorkDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_MASTER_WORK_DIR_NAME);

    Map<String, LocalResource> appMasterResources = Maps.newHashMap();

    if (this.config.hasPath(GobblinYarnConfigurationKeys.LIB_JARS_DIR_KEY)) {
      Path libJarsDestDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.LIB_JARS_DIR_NAME);
      addLibJars(new Path(this.config.getString(GobblinYarnConfigurationKeys.LIB_JARS_DIR_KEY)),
          Optional.of(appMasterResources), libJarsDestDir);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_JARS_KEY)) {
      Path appJarsDestDir = new Path(appMasterWorkDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME);
      addAppJars(this.config.getString(GobblinYarnConfigurationKeys.APP_MASTER_JARS_KEY),
          Optional.of(appMasterResources), appJarsDestDir);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_FILES_LOCAL_KEY)) {
      Path appFilesDestDir = new Path(appMasterWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME);
      addAppLocalFiles(this.config.getString(GobblinYarnConfigurationKeys.APP_MASTER_FILES_LOCAL_KEY),
          Optional.of(appMasterResources), appFilesDestDir);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_FILES_REMOTE_KEY)) {
      addAppRemoteFiles(this.config.getString(GobblinYarnConfigurationKeys.APP_MASTER_FILES_REMOTE_KEY),
          appMasterResources);
    }
    if (this.config.hasPath(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY)) {
      Path appFilesDestDir = new Path(appMasterWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME);
      addJobConfPackage(this.config.getString(GobblinClusterConfigurationKeys.JOB_CONF_PATH_KEY), appFilesDestDir,
          appMasterResources);
    }

    return appMasterResources;
  }

  private void addContainerLocalResources(ApplicationId applicationId) throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPath(this.fs, this.applicationName, applicationId.toString());
    Path containerWorkDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);

    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY)) {
      Path appJarsDestDir = new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME);
      addAppJars(this.config.getString(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY),
          Optional.<Map<String, LocalResource>>absent(), appJarsDestDir);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_FILES_LOCAL_KEY)) {
      Path appFilesDestDir = new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME);
      addAppLocalFiles(this.config.getString(GobblinYarnConfigurationKeys.CONTAINER_FILES_LOCAL_KEY),
          Optional.<Map<String, LocalResource>>absent(), appFilesDestDir);
    }
  }

  private void addLibJars(Path srcLibJarDir, Optional<Map<String, LocalResource>> resourceMap, Path destDir)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(this.yarnConfiguration);
    FileStatus[] libJarFiles = localFs.listStatus(srcLibJarDir);
    if (libJarFiles == null || libJarFiles.length == 0) {
      return;
    }

    for (FileStatus libJarFile : libJarFiles) {
      Path destFilePath = new Path(destDir, libJarFile.getPath().getName());
      this.fs.copyFromLocalFile(libJarFile.getPath(), destFilePath);
      if (resourceMap.isPresent()) {
        YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.FILE, resourceMap.get());
      }
    }
  }

  private void addAppJars(String jarFilePathList, Optional<Map<String, LocalResource>> resourceMap,
      Path destDir) throws IOException {
    for (String jarFilePath : SPLITTER.split(jarFilePathList)) {
      Path srcFilePath = new Path(jarFilePath);
      Path destFilePath = new Path(destDir, srcFilePath.getName());
      this.fs.copyFromLocalFile(srcFilePath, destFilePath);
      if (resourceMap.isPresent()) {
        YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.FILE, resourceMap.get());
      }
    }
  }

  private void addAppLocalFiles(String localFilePathList, Optional<Map<String, LocalResource>> resourceMap,
      Path destDir) throws IOException {
    for (String localFilePath : SPLITTER.split(localFilePathList)) {
      Path srcFilePath = new Path(localFilePath);
      Path destFilePath = new Path(destDir, srcFilePath.getName());
      this.fs.copyFromLocalFile(srcFilePath, destFilePath);
      if (resourceMap.isPresent()) {
        YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.FILE, resourceMap.get());
      }
    }
  }

  private void addAppRemoteFiles(String hdfsFileList, Map<String, LocalResource> resourceMap)
      throws IOException {
    for (String hdfsFilePath : SPLITTER.split(hdfsFileList)) {
      YarnHelixUtils.addFileAsLocalResource(this.fs, new Path(hdfsFilePath), LocalResourceType.FILE, resourceMap);
    }
  }

  private void addJobConfPackage(String jobConfPackagePath, Path destDir, Map<String, LocalResource> resourceMap)
      throws IOException {
    Path srcFilePath = new Path(jobConfPackagePath);
    Path destFilePath = new Path(destDir, srcFilePath.getName() + GobblinClusterConfigurationKeys.TAR_GZ_FILE_SUFFIX);
    StreamUtils.tar(FileSystem.getLocal(this.yarnConfiguration), this.fs, srcFilePath, destFilePath);
    YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.ARCHIVE, resourceMap);
  }

  private String buildApplicationMasterCommand(int memoryMbs) {
    String appMasterClassName = GobblinApplicationMaster.class.getSimpleName();
    return new StringBuilder()
        .append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
        .append(" -Xmx").append(memoryMbs).append("M")
        .append(" ").append(JvmUtils.formatJvmArguments(this.appMasterJvmArgs))
        .append(" ").append(GobblinApplicationMaster.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.applicationName)
        .append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
            appMasterClassName).append(".").append(ApplicationConstants.STDOUT)
        .append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
            appMasterClassName).append(".").append(ApplicationConstants.STDERR)
        .toString();
  }

  private void setupSecurityTokens(ContainerLaunchContext containerLaunchContext) throws IOException {
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    String tokenRenewer = this.yarnConfiguration.get(YarnConfiguration.RM_PRINCIPAL);
    if (tokenRenewer == null || tokenRenewer.length() == 0) {
      throw new IOException("Failed to get master Kerberos principal for the RM to use as renewer");
    }

    // For now, only getting tokens for the default file-system.
    Token<?> tokens[] = this.fs.addDelegationTokens(tokenRenewer, credentials);
    if (tokens != null) {
      for (Token<?> token : tokens) {
        LOGGER.info("Got delegation token for " + this.fs.getUri() + "; " + token);
      }
    }

    Closer closer = Closer.create();
    try {
      DataOutputBuffer dataOutputBuffer = closer.register(new DataOutputBuffer());
      credentials.writeTokenStorageToStream(dataOutputBuffer);
      ByteBuffer fsTokens = ByteBuffer.wrap(dataOutputBuffer.getData(), 0, dataOutputBuffer.getLength());
      containerLaunchContext.setTokens(fsTokens);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private LogCopier buildLogCopier(Config config, Path sinkLogDir, Path appWorkDir) throws IOException {
    FileSystem rawLocalFs = this.closer.register(new RawLocalFileSystem());
    rawLocalFs.initialize(URI.create(ConfigurationKeys.LOCAL_FS_URI), new Configuration());

    LogCopier.Builder builder = LogCopier.newBuilder()
            .useSrcFileSystem(this.fs)
            .useDestFileSystem(rawLocalFs)
            .readFrom(getHdfsLogDir(appWorkDir))
            .writeTo(sinkLogDir)
            .acceptsLogFileExtensions(ImmutableSet.of(ApplicationConstants.STDOUT, ApplicationConstants.STDERR));
    if (config.hasPath(GobblinYarnConfigurationKeys.LOG_COPIER_MAX_FILE_SIZE)) {
      builder.useMaxBytesPerLogFile(config.getBytes(GobblinYarnConfigurationKeys.LOG_COPIER_MAX_FILE_SIZE));
    }
    if (config.hasPath(GobblinYarnConfigurationKeys.LOG_COPIER_SCHEDULER)) {
      builder.useScheduler(config.getString(GobblinYarnConfigurationKeys.LOG_COPIER_SCHEDULER));
    }
    return builder.build();
  }

  private Path getHdfsLogDir(Path appWorkDir) throws IOException {
    Path logRootDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_LOGS_DIR_NAME);
    if (!this.fs.exists(logRootDir)) {
      this.fs.mkdirs(logRootDir);
    }

    return logRootDir;
  }

  private YarnAppSecurityManager buildYarnAppSecurityManager() throws IOException {
    Path tokenFilePath = new Path(this.fs.getHomeDirectory(), this.applicationName + Path.SEPARATOR +
        GobblinYarnConfigurationKeys.TOKEN_FILE_NAME);
    return new YarnAppSecurityManager(this.config, this.helixManager, this.fs, tokenFilePath);
  }

  @VisibleForTesting
  void sendShutdownRequest() {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    criteria.setSessionSpecific(true);

    Message shutdownRequest = new Message(GobblinHelixConstants.SHUTDOWN_MESSAGE_TYPE,
        HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString().toLowerCase() + UUID.randomUUID().toString());
    shutdownRequest.setMsgSubType(HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());
    shutdownRequest.setMsgState(Message.MessageState.NEW);
    shutdownRequest.setTgtSessionId("*");

    int messagesSent = this.helixManager.getMessagingService().send(criteria, shutdownRequest);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the controller", shutdownRequest.getMsgSubType()));
    }
  }

  @VisibleForTesting
  void cleanUpAppWorkDirectory(ApplicationId applicationId) throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPath(this.fs, this.applicationName, applicationId.toString());
    if (this.fs.exists(appWorkDir)) {
      LOGGER.info("Deleting application working directory " + appWorkDir);
      this.fs.delete(appWorkDir, true);
    }
  }

  private void sendEmailOnShutdown(Optional<ApplicationReport> applicationReport) {
    String subject = String.format("Gobblin Yarn application %s completed", this.applicationName);

    StringBuilder messageBuilder = new StringBuilder("Gobblin Yarn ApplicationReport:");
    if (applicationReport.isPresent()) {
      messageBuilder.append("\n");
      messageBuilder.append("\tApplication ID: ").append(applicationReport.get().getApplicationId()).append("\n");
      messageBuilder.append("\tApplication attempt ID: ")
          .append(applicationReport.get().getCurrentApplicationAttemptId()).append("\n");
      messageBuilder.append("\tFinal application status: ").append(applicationReport.get().getFinalApplicationStatus())
          .append("\n");
      messageBuilder.append("\tStart time: ").append(applicationReport.get().getStartTime()).append("\n");
      messageBuilder.append("\tFinish time: ").append(applicationReport.get().getFinishTime()).append("\n");

      if (!Strings.isNullOrEmpty(applicationReport.get().getDiagnostics())) {
        messageBuilder.append("\tDiagnostics: ").append(applicationReport.get().getDiagnostics()).append("\n");
      }

      ApplicationResourceUsageReport resourceUsageReport = applicationReport.get().getApplicationResourceUsageReport();
      if (resourceUsageReport != null) {
        messageBuilder.append("\tUsed containers: ").append(resourceUsageReport.getNumUsedContainers()).append("\n");
        Resource usedResource = resourceUsageReport.getUsedResources();
        if (usedResource != null) {
          messageBuilder.append("\tUsed memory (MBs): ").append(usedResource.getMemory()).append("\n");
          messageBuilder.append("\tUsed vcores: ").append(usedResource.getVirtualCores()).append("\n");
        }
      }
    } else {
      messageBuilder.append(' ').append("Not available");
    }

    try {
      EmailUtils.sendEmail(ConfigUtils.configToState(this.config), subject, messageBuilder.toString());
    } catch (EmailException ee) {
      LOGGER.error("Failed to send email notification on shutdown", ee);
    }
  }

  public static void main(String[] args) throws Exception {
    final GobblinYarnAppLauncher gobblinYarnAppLauncher =
        new GobblinYarnAppLauncher(ConfigFactory.load(), new YarnConfiguration());
    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          gobblinYarnAppLauncher.stop();
        } catch (IOException ioe) {
          LOGGER.error("Failed to shutdown the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
        } catch (TimeoutException te) {
          LOGGER.error("Timeout in stopping the service manager", te);
        } finally {
          if (gobblinYarnAppLauncher.emailNotificationOnShutdown) {
            gobblinYarnAppLauncher.sendEmailOnShutdown(Optional.<ApplicationReport>absent());
          }
        }
      }
    });

    gobblinYarnAppLauncher.launch();
  }
}
