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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.Criteria;
import org.apache.helix.HelixConnection;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ExecutorsUtils;


/**
 * A client driver to launch Gobblin as a Yarn application.
 *
 * <p>
 *   This class starts the {@link GobblinApplicationMaster}. Once the application is launched, this class
 *   periodically polls the status of the application through a {@link ListeningExecutorService}. If a
 *   shutdown signal is received, it sends a Helix {@link org.apache.helix.model.Message.MessageType#SCHEDULER_MSG}
 *   to the {@link GobblinApplicationMaster} asking it to shutdown and release all the allocated containers.
 * </p>
 *
 * @author ynli
 */
public class GobblinYarnAppLauncher {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinYarnAppLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final String appName;
  private final String appQueueName;

  private final Config config;

  private final HelixManager helixManager;

  private final ServiceManager serviceManager;

  private final Configuration yarnConfiguration;
  private final YarnClient yarnClient;
  private final FileSystem fs;

  private final ListeningExecutorService applicationStatusMonitor;

  private final String appMasterJvmArgs;

  // Yarn application ID
  private volatile Optional<ApplicationId> applicationId = Optional.absent();

  // This flag tells if the Yarn application has already completed. This is used to
  // tell if it is necessary to send a shutdown message to the ApplicationMaster.
  private volatile boolean applicationCompleted = false;

  private volatile boolean stopped = false;

  public GobblinYarnAppLauncher(Config config) throws IOException {
    this.config = config;

    this.appName = config.getString(ConfigurationConstants.APPLICATION_NAME_KEY);
    this.appQueueName = config.getString(ConfigurationConstants.APP_QUEUE_KEY);

    String zkConnectionString = config.getString(ConfigurationConstants.ZK_CONNECTION_STRING_KEY);
    LOGGER.info("Using ZooKeeper connection string: " + zkConnectionString);

    this.helixManager = HelixManagerFactory.getZKHelixManager(
        config.getString(ConfigurationConstants.HELIX_CLUSTER_NAME_KEY), YarnHelixUtils.getHostname(),
        InstanceType.SPECTATOR, zkConnectionString);

    this.yarnConfiguration = new YarnConfiguration();
    this.yarnConfiguration.set("fs.automatic.close", "false");
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConfiguration);

    this.fs = config.hasPath(ConfigurationKeys.FS_URI_KEY) ?
        FileSystem.get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), this.yarnConfiguration) :
        FileSystem.get(this.yarnConfiguration);

    List<Service> services = Lists.newArrayList();
    if (UserGroupInformation.isSecurityEnabled()) {
      LOGGER.info("Adding YarnAppSecurityManager since security is enabled");
      services.add(new YarnAppSecurityManager(config, this.helixManager, this.fs));
    }
    this.serviceManager = new ServiceManager(services);

    this.applicationStatusMonitor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("GobblinYarnAppStatusMonitor"))));

    this.appMasterJvmArgs = Strings.nullToEmpty(config.getString(ConfigurationConstants.APP_MASTER_JVM_ARGS_KEY));
  }

  /**
   * Launch a new Gobblin instance on Yarn.
   *
   * @throws IOException if there's something wrong launching the application
   * @throws YarnException if there's something wrong launching the application
   */
  public void launch() throws IOException, YarnException {
    createGobblinYarnHelixCluster();

    try {
      this.helixManager.connect();
    } catch (Exception e) {
      LOGGER.error("HelixManager failed to connect", e);
      throw Throwables.propagate(e);
    }

    // Start all the services running in the ApplicationMaster
    this.serviceManager.startAsync();

    this.yarnClient.start();
    this.applicationId = Optional.of(setupAndSubmitApplication());

    ListenableFuture<ApplicationReport> appReportFuture =
        this.applicationStatusMonitor.submit(new Callable<ApplicationReport>() {

          @Override
          public ApplicationReport call()
              throws IOException, YarnException {
            while (true) {
              try {
                ApplicationReport appReport = yarnClient.getApplicationReport(applicationId.get());
                YarnApplicationState appState = appReport.getYarnApplicationState();
                if (appState == YarnApplicationState.FINISHED ||
                    appState == YarnApplicationState.FAILED ||
                    appState == YarnApplicationState.KILLED) {
                  applicationCompleted = true;
                  return appReport;
                }

                LOGGER.info("Gobblin Yarn application state: " + appState.toString());

                Thread.sleep(TimeUnit.SECONDS.toMillis(60));
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              }
            }
          }
        });

    Futures.addCallback(appReportFuture, new FutureCallback<ApplicationReport>() {
      @Override
      public void onSuccess(ApplicationReport applicationReport) {
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
        }
      }

      @Override
      public void onFailure(@Nonnull Throwable t) {
        LOGGER.error("Failed to get ApplicationReport due to: " + t);
        try {
          GobblinYarnAppLauncher.this.stop();
        } catch (IOException ioe) {
          LOGGER.error("Failed to close the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
        }
      }
    });
  }

  public synchronized void stop() throws IOException {
    if (this.stopped) {
      return;
    }

    LOGGER.info("Stopping the " + GobblinYarnAppLauncher.class.getSimpleName());

    try {
      if (this.applicationId.isPresent() && !this.applicationCompleted) {
        // Only send the shutdown message if the application has been successfully submitted and is still running
        sendShutdownRequest();
      }

      ExecutorsUtils.shutdownExecutorService(this.applicationStatusMonitor, Optional.of(LOGGER), 5, TimeUnit.MINUTES);

      this.yarnClient.stop();

      if (this.helixManager.isConnected()) {
        this.helixManager.disconnect();
      }
    } finally {
      try {
        if (this.applicationId.isPresent()) {
          cleanUpAppWorkDirectory(this.applicationId.get());
        }
      } finally {
        this.fs.close();
      }
    }

    this.stopped = true;
  }

  /**
   * Create a Helix cluster with the cluster name specified using
   * {@link ConfigurationConstants#HELIX_CLUSTER_NAME_KEY} if it does not exist.
   */
  private void createGobblinYarnHelixCluster() {
    HelixConnection helixConnection =
        new ZkHelixConnection(this.config.getString(ConfigurationConstants.ZK_CONNECTION_STRING_KEY));
    helixConnection.connect();
    try {
      ClusterId clusterId = ClusterId.from(this.config.getString(ConfigurationConstants.HELIX_CLUSTER_NAME_KEY));
      ClusterConfig clusterConfig = new ClusterConfig.Builder(clusterId)
          .addStateModelDefinition(
              new StateModelDefinition(StateModelConfigGenerator.generateConfigForTaskStateModel()))
          .autoJoin(true)
          .build();
      boolean clusterCreated = helixConnection.createClusterAccessor(clusterId).createCluster(clusterConfig);
      if (clusterCreated) {
        LOGGER.info("Created Helix cluster " + clusterId.stringify());
      } else {
        LOGGER.info("Helix cluster " + clusterId.stringify() + " already exists");
      }
    } finally {
      helixConnection.disconnect();
    }
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

    GetNewApplicationResponse newApplicationResponse =gobblinYarnApp.getNewApplicationResponse();
    // Set up resource type requirements for ApplicationMaster
    Resource resource = prepareContainerResource(newApplicationResponse);

    Map<String, LocalResource> appMasterLocalResources = Maps.newHashMap();
    // Add lib jars, and jars and files that the ApplicationMaster need as LocalResources
    addAppMasterLocalResources(applicationId, appMasterLocalResources);

    ContainerLaunchContext amContainerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    amContainerLaunchContext.setLocalResources(appMasterLocalResources);
    amContainerLaunchContext.setEnvironment(getEnvironmentVariables());
    amContainerLaunchContext.setCommands(Lists.newArrayList(buildApplicationMasterCommand(resource.getMemory())));
    if (UserGroupInformation.isSecurityEnabled()) {
      setupSecurityTokens(amContainerLaunchContext);
    }

    // Setup the application submission context
    appSubmissionContext.setApplicationName(this.appName);
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
    int memoryMbs = this.config.getInt(ConfigurationConstants.APP_MASTER_MEMORY_MBS_KEY);
    int maximumMemoryCapacity = newApplicationResponse.getMaximumResourceCapability().getMemory();
    if (memoryMbs > maximumMemoryCapacity) {
      LOGGER.info(String.format("Specified AM memory [%d] is above the maximum memory capacity [%d] of the "
          + "cluster, using the maximum memory capacity instead.", memoryMbs, maximumMemoryCapacity));
      memoryMbs = maximumMemoryCapacity;
    }

    int vCores = this.config.getInt(ConfigurationConstants.APP_MASTER_CORES_KEY);
    int maximumVirtualCoreCapacity = newApplicationResponse.getMaximumResourceCapability().getVirtualCores();
    if (vCores > maximumVirtualCoreCapacity) {
      LOGGER.info(String.format("Specified AM vcores [%d] is above the maximum vcore capacity [%d] of the "
          + "cluster, using the maximum vcore capacity instead.", memoryMbs, maximumMemoryCapacity));
      vCores = maximumVirtualCoreCapacity;
    }

    // Set up resource type requirements for ApplicationMaster
    return Resource.newInstance(memoryMbs, vCores);
  }

  private void addAppMasterLocalResources(ApplicationId applicationId,
      Map<String, LocalResource> resources) throws IOException {
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.appName, applicationId);
    Path appMasterWorkDir = new Path(appWorkDir, ConfigurationConstants.APP_MASTER_WORK_DIR_NAME);

    if (this.config.hasPath(ConfigurationConstants.LIB_JARS_DIR_KEY)) {
      Path libJarsDestDir = new Path(appWorkDir, ConfigurationConstants.LIB_JARS_DIR_NAME);
      addLibJars(new Path(this.config.getString(ConfigurationConstants.LIB_JARS_DIR_KEY)),
          Optional.of(resources), libJarsDestDir);
    }
    if (this.config.hasPath(ConfigurationConstants.APP_MASTER_JARS_KEY)) {
      Path appJarsDestDir = new Path(appMasterWorkDir, ConfigurationConstants.APP_JARS_DIR_NAME);
      addAppJars(this.config.getString(ConfigurationConstants.APP_MASTER_JARS_KEY),
          Optional.of(resources), appJarsDestDir);
    }
    if (this.config.hasPath(ConfigurationConstants.APP_MASTER_FILES_LOCAL_KEY)) {
      Path appFilesDestDir = new Path(appMasterWorkDir, ConfigurationConstants.APP_FILES_DIR_NAME);
      addAppLocalFiles(this.config.getString(ConfigurationConstants.APP_MASTER_FILES_LOCAL_KEY),
          Optional.of(resources), appFilesDestDir);
    }
    if (this.config.hasPath(ConfigurationConstants.APP_MASTER_FILES_REMOTE_KEY)) {
      addAppRemoteFiles(this.config.getString(ConfigurationConstants.APP_MASTER_FILES_REMOTE_KEY), resources);
    }
    if (this.config.hasPath(ConfigurationConstants.JOB_CONF_PACKAGE_PATH_KEY)) {
      Path appFilesDestDir = new Path(appMasterWorkDir, ConfigurationConstants.APP_FILES_DIR_NAME);
      addJobConfPackage(this.config.getString(ConfigurationConstants.JOB_CONF_PACKAGE_PATH_KEY),
          appFilesDestDir, resources);
    }
  }

  private void addContainerLocalResources(ApplicationId applicationId) throws IOException {
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.appName, applicationId);
    Path containerWorkDir = new Path(appWorkDir, ConfigurationConstants.CONTAINER_WORK_DIR_NAME);

    if (this.config.hasPath(ConfigurationConstants.CONTAINER_JARS_KEY)) {
      Path appJarsDestDir = new Path(containerWorkDir, ConfigurationConstants.APP_JARS_DIR_NAME);
      addAppJars(this.config.getString(ConfigurationConstants.CONTAINER_JARS_KEY),
          Optional.<Map<String, LocalResource>>absent(), appJarsDestDir);
    }
    if (this.config.hasPath(ConfigurationConstants.CONTAINER_FILES_LOCAL_KEY)) {
      Path appFilesDestDir = new Path(containerWorkDir, ConfigurationConstants.APP_FILES_DIR_NAME);
      addAppLocalFiles(this.config.getString(ConfigurationConstants.CONTAINER_FILES_LOCAL_KEY),
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
        addFileAsLocalResource(destFilePath, LocalResourceType.FILE, resourceMap.get());
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
        addFileAsLocalResource(destFilePath, LocalResourceType.FILE, resourceMap.get());
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
        addFileAsLocalResource(destFilePath, LocalResourceType.FILE, resourceMap.get());
      }
    }
  }

  private void addAppRemoteFiles(String hdfsFileList, Map<String, LocalResource> resourceMap)
      throws IOException {
    for (String hdfsFilePath : SPLITTER.split(hdfsFileList)) {
      addFileAsLocalResource(new Path(hdfsFilePath), LocalResourceType.FILE, resourceMap);
    }
  }

  private void addJobConfPackage(String jobConfPackagePath, Path destDir, Map<String, LocalResource> resourceMap)
      throws IOException {
    Path srcFilePath = new Path(jobConfPackagePath);
    Path destFilePath = new Path(destDir, srcFilePath.getName());
    this.fs.copyFromLocalFile(srcFilePath, destFilePath);
    addFileAsLocalResource(destFilePath, LocalResourceType.ARCHIVE, resourceMap);
  }

  private void addFileAsLocalResource(Path destFilePath, LocalResourceType resourceType,
      Map<String, LocalResource> resourceMap) throws IOException {
    LocalResource fileResource = Records.newRecord(LocalResource.class);
    FileStatus fileStatus = this.fs.getFileStatus(destFilePath);
    fileResource.setResource(ConverterUtils.getYarnUrlFromPath(destFilePath));
    fileResource.setSize(fileStatus.getLen());
    fileResource.setTimestamp(fileStatus.getModificationTime());
    fileResource.setType(resourceType);
    fileResource.setVisibility(LocalResourceVisibility.APPLICATION);
    LOGGER.debug(String.format("Created a LocalResource for file %s of type %s",
        fileResource.getResource(), resourceType));
    resourceMap.put(destFilePath.getName(), fileResource);
  }

  private Map<String, String> getEnvironmentVariables() {
    Map<String, String> environmentVariableMap = Maps.newHashMap();

    Apps.addToEnvironment(environmentVariableMap, ApplicationConstants.Environment.JAVA_HOME.key(),
        System.getenv(ApplicationConstants.Environment.JAVA_HOME.key()));

    // Add jars/files in the working directory of the ApplicationMaster to the CLASSPATH
    Apps.addToEnvironment(environmentVariableMap, ApplicationConstants.Environment.CLASSPATH.key(),
        ApplicationConstants.Environment.PWD.$());
    Apps.addToEnvironment(environmentVariableMap, ApplicationConstants.Environment.CLASSPATH.key(),
        ApplicationConstants.Environment.PWD.$() + File.separator + "*");

    String[] classpaths = this.yarnConfiguration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH);
    if (classpaths != null) {
      for (String classpath : classpaths) {
        Apps.addToEnvironment(
            environmentVariableMap, ApplicationConstants.Environment.CLASSPATH.key(), classpath.trim());
      }
    }

    return environmentVariableMap;
  }

  private String buildApplicationMasterCommand(int memoryMbs) {
    String appMasterClassName = GobblinApplicationMaster.class.getSimpleName();
    return String.format(
        "%s/bin/java -Xmx%dM %s %s" +
        " --%s %s" +
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "%s.%s" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "%s.%s",
        ApplicationConstants.Environment.JAVA_HOME.$(), memoryMbs, this.appMasterJvmArgs,
        GobblinApplicationMaster.class.getName(), ConfigurationConstants.APPLICATION_NAME_OPTION_NAME,
        this.appName, appMasterClassName, ApplicationConstants.STDOUT, appMasterClassName,
        ApplicationConstants.STDERR);
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
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.appName, applicationId);
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
        }
      }
    });

    gobblinYarnAppLauncher.launch();
  }
}
