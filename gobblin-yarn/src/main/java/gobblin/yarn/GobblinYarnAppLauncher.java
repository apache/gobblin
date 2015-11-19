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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
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

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ExecutorsUtils;
import gobblin.util.logs.LogCopier;
import gobblin.yarn.event.ApplicationReportArrivalEvent;


/**
 * A client driver to launch Gobblin as a Yarn application.
 *
 * <p>
 *   This class, upon starting, will check if there's a Yarn application that it has previously submitted and
 *   it is able to reconnect to. More specifically, it checks if the file storing the previous application ID
 *   exists and if so, reads the previous application ID. It then checks if the application with that ID can
 *   be reconnected to, i.e., if the application has not completed yet. If so, it simply starts monitoring
 *   that application.
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
 *   asking it to shutdown and release all the allocated containers. Upon successfully shutting down the Yarn
 *   application, it deletes the file storing the application ID and cleans up the working directory.
 * </p>
 *
 * @author ynli
 */
public class GobblinYarnAppLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinYarnAppLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  // The set of Yarn application types this class is interested in. This is used to
  // lookup the application this class has launched previously upon restarting.
  private static final Set<String> APPLICATION_TYPES = ImmutableSet.of("YARN");

  // The set of Yarn application states under which the driver can reconnect to the Yarn application after restart
  private static final EnumSet<YarnApplicationState> RECONNECTABLE_APPLICATION_STATES = EnumSet.of(
      YarnApplicationState.NEW,
      YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED,
      YarnApplicationState.ACCEPTED,
      YarnApplicationState.RUNNING
  );

  private static final String APPLICATION_ID_FILE_NAME = "_application.id";

  private final String applicationName;
  private final String appQueueName;

  private final Config config;

  private final HelixManager helixManager;

  private final Configuration yarnConfiguration;
  private final YarnClient yarnClient;
  private final FileSystem fs;

  private final ApplicationIdStore applicationIdStore;

  private final EventBus eventBus = new EventBus(GobblinYarnAppLauncher.class.getSimpleName());

  private final ScheduledExecutorService applicationStatusMonitor;
  private final long appReportIntervalMinutes;

  private final Optional<String> appMasterJvmArgs;

  private final Path sinkLogRootDir;

  private final Closer fsCloser = Closer.create();

  // Yarn application ID
  private volatile Optional<ApplicationId> applicationId = Optional.absent();

  private volatile Optional<ServiceManager> serviceManager = Optional.absent();

  // This flag tells if the Yarn application has already completed. This is used to
  // tell if it is necessary to send a shutdown message to the ApplicationMaster.
  private volatile boolean applicationCompleted = false;

  private volatile boolean stopped = false;

  public GobblinYarnAppLauncher(Config config) throws IOException {
    this.config = config;

    this.applicationName = config.getString(GobblinYarnConfigurationKeys.APPLICATION_NAME_KEY);
    this.appQueueName = config.getString(GobblinYarnConfigurationKeys.APP_QUEUE_KEY);

    String zkConnectionString = config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY);
    LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY), YarnHelixUtils.getHostname(),
        InstanceType.SPECTATOR, zkConnectionString);

    this.yarnConfiguration = new YarnConfiguration();
    this.yarnConfiguration.set("fs.automatic.close", "false");
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConfiguration);

    this.fs = config.hasPath(ConfigurationKeys.FS_URI_KEY) ?
        FileSystem.get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), this.yarnConfiguration) :
        FileSystem.get(this.yarnConfiguration);
    this.fsCloser.register(this.fs);

    this.applicationIdStore = new FsApplicationIdStore(this.fs,
        new Path(YarnHelixUtils.getAppRootDirPath(this.fs, this.applicationName), APPLICATION_ID_FILE_NAME));

    this.applicationStatusMonitor = Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("GobblinYarnAppStatusMonitor")));
    this.appReportIntervalMinutes = config.getLong(GobblinYarnConfigurationKeys.APP_REPORT_INTERVAL_MINUTES_KEY);

    this.appMasterJvmArgs = config.hasPath(GobblinYarnConfigurationKeys.APP_MASTER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinYarnConfigurationKeys.APP_MASTER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    this.sinkLogRootDir = new Path(config.getString(GobblinYarnConfigurationKeys.LOGS_SINK_ROOT_DIR_KEY));
  }

  /**
   * Launch a new Gobblin instance on Yarn.
   *
   * @throws IOException if there's something wrong launching the application
   * @throws YarnException if there's something wrong launching the application
   */
  public void launch() throws IOException, YarnException {
    this.eventBus.register(this);

    createGobblinYarnHelixCluster();

    try {
      this.helixManager.connect();
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }

    this.yarnClient.start();
    this.applicationId = getApplicationId();
    this.applicationIdStore.put(this.applicationId.get().toString());

    this.applicationStatusMonitor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          eventBus.post(new ApplicationReportArrivalEvent(yarnClient.getApplicationReport(applicationId.get())));
        } catch (YarnException | IOException e) {
          LOGGER.error("Failed to get application report for Gobblin Yarn application " + applicationId.get(), e);
        }
      }
    }, 0, this.appReportIntervalMinutes, TimeUnit.MINUTES);

    List<Service> services = Lists.newArrayList();
    if (config.hasPath(GobblinYarnConfigurationKeys.KEYTAB_FILE_PATH)) {
      LOGGER.info("Adding YarnAppSecurityManager since login is keytab based");
      services.add(new YarnAppSecurityManager(config, this.helixManager, this.fs));
    }
    services.add(buildLogCopier(
        new Path(this.sinkLogRootDir, this.applicationName + Path.SEPARATOR + this.applicationId.get().toString()),
        YarnHelixUtils.getAppWorkDirPath(this.fs, this.applicationName, this.applicationId.get())));

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

      this.yarnClient.stop();

      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }

      // Delete the file storing the application ID if the application has been shutdown successfully
      this.applicationIdStore.delete();
    } finally {
      try {
        if (this.applicationId.isPresent()) {
          cleanUpAppWorkDirectory(this.applicationId.get());
        }
      } finally {
        this.fsCloser.close();
      }
    }

    this.stopped = true;
  }

  @Subscribe
  @SuppressWarnings("unused")
  public void handleApplicationReportArrivalEvent(ApplicationReportArrivalEvent applicationReportArrivalEvent) {
    ApplicationReport applicationReport = applicationReportArrivalEvent.getApplicationReport();

    YarnApplicationState appState = applicationReport.getYarnApplicationState();
    LOGGER.info("Gobblin Yarn application state: " + appState.toString());

    if (appState == YarnApplicationState.FINISHED ||
        appState == YarnApplicationState.FAILED ||
        appState == YarnApplicationState.KILLED) {

      applicationCompleted = true;

      LOGGER.info("Gobblin Yarn application finished with final status: " +
          applicationReport.getFinalApplicationStatus().toString());
      if (applicationReport.getFinalApplicationStatus() == FinalApplicationStatus.FAILED) {
        LOGGER.error(
            "Gobblin Yarn application failed for the following reason: " + applicationReport.getDiagnostics());
      }

      try {
        GobblinYarnAppLauncher.this.stop();
      } catch (IOException ioe) {
        LOGGER.error("Failed to close the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
      } catch (TimeoutException te) {
        LOGGER.error("Timeout in stopping the service manager", te);
      }
    }
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

  private Optional<ApplicationId> getReconnectableApplicationId() throws YarnException, IOException {
    Optional<String> existingApplicationId = this.applicationIdStore.get();
    if (!existingApplicationId.isPresent()) {
      return Optional.absent();
    }

    List<ApplicationReport> applicationReports =
        this.yarnClient.getApplications(APPLICATION_TYPES, RECONNECTABLE_APPLICATION_STATES);
    if (applicationReports == null || applicationReports.isEmpty()) {
      return Optional.absent();
    }

    for (ApplicationReport applicationReport : applicationReports) {
      if (applicationReport.getApplicationId().toString().equals(existingApplicationId.get())) {
        return Optional.of(applicationReport.getApplicationId());
      }
    }

    return Optional.absent();
  }

  /**
   * Create a Helix cluster with the cluster name specified using
   * {@link GobblinYarnConfigurationKeys#HELIX_CLUSTER_NAME_KEY} if it does not exist.
   */
  private void createGobblinYarnHelixCluster() {
    ClusterSetup clusterSetup =
        new ClusterSetup(this.config.getString(GobblinYarnConfigurationKeys.ZK_CONNECTION_STRING_KEY));
    String clusterName = this.config.getString(GobblinYarnConfigurationKeys.HELIX_CLUSTER_NAME_KEY);
    // Create the cluster and overwrite if it already exists
    clusterSetup.addCluster(clusterName, true);
    // Helix 0.6.x requires a configuration property to have the form key=value.
    String autoJoinConfig = ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN + "=true";
    clusterSetup.setConfig(HelixConfigScope.ConfigScopeProperty.CLUSTER, clusterName, autoJoinConfig);
    LOGGER.info("Created Helix cluster " + clusterName);
  }

  /**
   * Setup and submit the Gobblin Yarn application.
   *
   * @throws IOException if there's anything wrong setting up and submitting the Yarn application
   * @throws YarnException if there's anything wrong setting up and submitting the Yarn application
   */
  private ApplicationId setupAndSubmitApplication() throws IOException, YarnException {
    YarnClientApplication gobblinYarnApp = this.yarnClient.createApplication();
    ApplicationSubmissionContext appSubmissionContext = gobblinYarnApp.getApplicationSubmissionContext();
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
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.applicationName, applicationId);
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
    if (this.config.hasPath(GobblinYarnConfigurationKeys.JOB_CONF_PACKAGE_PATH_KEY)) {
      Path appFilesDestDir = new Path(appMasterWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME);
      addJobConfPackage(this.config.getString(GobblinYarnConfigurationKeys.JOB_CONF_PACKAGE_PATH_KEY), appFilesDestDir,
          appMasterResources);
    }

    return appMasterResources;
  }

  private void addContainerLocalResources(ApplicationId applicationId) throws IOException {
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.applicationName, applicationId);
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
    Path destFilePath = new Path(destDir, srcFilePath.getName());
    this.fs.copyFromLocalFile(srcFilePath, destFilePath);
    YarnHelixUtils.addFileAsLocalResource(this.fs, destFilePath, LocalResourceType.ARCHIVE, resourceMap);
  }

  private String buildApplicationMasterCommand(int memoryMbs) {
    String appMasterClassName = GobblinApplicationMaster.class.getSimpleName();
    return new StringBuilder()
        .append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
        .append(" -Xmx").append(memoryMbs).append("M")
        .append(" ").append(this.appMasterJvmArgs.or(""))
        .append(" ").append(GobblinApplicationMaster.class.getName())
        .append(" --").append(GobblinYarnConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
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

  private LogCopier buildLogCopier(Path sinkLogDir, Path appWorkDir) throws IOException {
    FileSystem rawLocalFs = this.fsCloser.register(new RawLocalFileSystem());
    rawLocalFs.initialize(URI.create(ConfigurationKeys.LOCAL_FS_URI), new Configuration());

    return LogCopier.newBuilder()
        .useSrcFileSystem(this.fs)
        .useDestFileSystem(rawLocalFs)
        .readFrom(getHdfsLogDir(appWorkDir))
        .writeTo(sinkLogDir)
        .acceptsLogFileExtensions(ImmutableSet.of(ApplicationConstants.STDOUT, ApplicationConstants.STDERR))
        .build();
  }

  private Path getHdfsLogDir(Path appWorkDir) throws IOException {
    Path logRootDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.APP_LOGS_DIR_NAME);
    if (!this.fs.exists(logRootDir)) {
      this.fs.mkdirs(logRootDir);
    }

    return logRootDir;
  }

  private void sendShutdownRequest() {
    Criteria criteria = new Criteria();
    criteria.setInstanceName("%");
    criteria.setResource("%");
    criteria.setPartition("%");
    criteria.setPartitionState("%");
    criteria.setRecipientInstanceType(InstanceType.CONTROLLER);
    criteria.setSessionSpecific(true);

    Message shutdownRequest = new Message(Message.MessageType.SHUTDOWN,
        HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString().toLowerCase() + UUID.randomUUID().toString());
    shutdownRequest.setMsgSubType(HelixMessageSubTypes.APPLICATION_MASTER_SHUTDOWN.toString());
    shutdownRequest.setMsgState(Message.MessageState.NEW);
    shutdownRequest.setTgtSessionId("*");

    int messagesSent = this.helixManager.getMessagingService().send(criteria, shutdownRequest);
    if (messagesSent == 0) {
      LOGGER.error(String.format("Failed to send the %s message to the controller", shutdownRequest.getMsgSubType()));
    }
  }

  private void cleanUpAppWorkDirectory(ApplicationId applicationId) throws IOException {
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.applicationName, applicationId);
    if (this.fs.exists(appWorkDir)) {
      LOGGER.info("Deleting application working directory " + appWorkDir);
      this.fs.delete(appWorkDir, true);
    }
  }

  public static void main(String[] args) throws Exception {
    final GobblinYarnAppLauncher gobblinYarnAppLauncher = new GobblinYarnAppLauncher(ConfigFactory.load());
    Runtime.getRuntime().addShutdownHook(new Thread() {

      @Override
      public void run() {
        try {
          gobblinYarnAppLauncher.stop();
        } catch (IOException ioe) {
          LOGGER.error("Failed to shutdown the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
        } catch (TimeoutException te) {
          LOGGER.error("Timeout in stopping the service manager", te);
        }
      }
    });

    gobblinYarnAppLauncher.launch();
  }
}
