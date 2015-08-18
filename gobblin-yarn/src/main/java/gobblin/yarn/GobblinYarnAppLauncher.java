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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.apache.helix.HelixConnection;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ExecutorsUtils;


/**
 * A client driver to launch Gobblin on Yarn.
 *
 * <p>
 *   This class starts the {@link GobblinApplicationMaster}.
 * </p>
 *
 * @author ynli
 */
public class GobblinYarnAppLauncher implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinYarnAppLauncher.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final String appName;
  private final String appQueueName;

  private final Config config;
  private final Configuration yarnConfiguration;
  private final YarnClient yarnClient;
  private final ExecutorService applicationStatusMonitor;
  private final FileSystem fs;

  private volatile ApplicationId applicationId = null;

  public GobblinYarnAppLauncher(Config config) throws IOException, YarnException {
    this.appName = config.hasPath(ConfigurationConstants.APPLICATION_NAME_KEY) ?
        config.getString(ConfigurationConstants.APPLICATION_NAME_KEY) :
        ConfigurationConstants.DEFAULT_APPLICATION_NAME;
    this.appQueueName = config.hasPath(ConfigurationConstants.APP_QUEUE_KEY) ?
        config.getString(ConfigurationConstants.APP_QUEUE_KEY) : ConfigurationConstants.DEFAULT_APP_QUEUE;

    this.config = config;
    this.yarnConfiguration = new YarnConfiguration();
    this.yarnClient = YarnClient.createYarnClient();
    this.yarnClient.init(this.yarnConfiguration);

    this.fs = config.hasPath(ConfigurationKeys.FS_URI_KEY) ?
        FileSystem.get(URI.create(config.getString(ConfigurationKeys.FS_URI_KEY)), this.yarnConfiguration) :
        FileSystem.get(this.yarnConfiguration);

    this.applicationStatusMonitor = Executors.newSingleThreadExecutor(
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("GobblinYarnAppStatusMonitor")));
  }

  /**
   * Launch a new Gobblin instance on Yarn.
   *
   * @return an {@link ApplicationReport} instance
   * @throws IOException if there's something wrong launching the application
   * @throws YarnException if there's something wrong launching the application
   */
  public ApplicationReport launch() throws IOException, YarnException {
    createGobblinYarnHelixCluster();

    this.yarnClient.start();
    this.applicationId = setupAndSubmitApplication();

    Future<ApplicationReport> result = this.applicationStatusMonitor.submit(new Callable<ApplicationReport>() {

      @Override
      public ApplicationReport call() throws IOException, YarnException, InterruptedException {
        while (true) {
          ApplicationReport appReport = yarnClient.getApplicationReport(applicationId);
          YarnApplicationState appState = appReport.getYarnApplicationState();
          if (appState == YarnApplicationState.FINISHED ||
              appState == YarnApplicationState.FAILED ||
              appState == YarnApplicationState.KILLED) {
            LOGGER.info("Gobblin Yarn application finished with final status: " +
                appReport.getFinalApplicationStatus().toString());
            if (appReport.getFinalApplicationStatus() == FinalApplicationStatus.FAILED) {
              LOGGER.error("Gobblin Yarn application failed for the following reason: " + appReport.getDiagnostics());
            }
            return appReport;
          }

          LOGGER.info("Gobblin Yarn application state: " + appState.toString());
          Thread.sleep(TimeUnit.SECONDS.toMillis(60));
        }
      }});

    try {
      return result.get();
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    } finally {
      cleanUpAppWorkDirectory(applicationId);
    }
  }

  @Override
  public void close() throws IOException {
    ExecutorsUtils.shutdownExecutorService(this.applicationStatusMonitor);

    try {
      if (this.applicationId != null
          && this.yarnClient.getApplicationReport(this.applicationId).getYarnApplicationState()
          == YarnApplicationState.RUNNING) {
        this.yarnClient.killApplication(this.applicationId);
      }
    } catch (YarnException ye) {
      LOGGER.error("Failed to kill the Yarn application " + this.applicationId);
    } finally {
      this.yarnClient.stop();
    }
  }

  /**
   * Create a Helix cluster with the cluster name specified using
   * {@link ConfigurationConstants#HELIX_CLUSTER_NAME_KEY} if it does not eixst.
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

    ContainerLaunchContext amContainerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    Map<String, LocalResource> appMasterLocalResources = Maps.newHashMap();
    // Add lib jars, and jars and files that the ApplicationMaster need as LocalResources
    addAppMasterLocalResources(applicationId, appMasterLocalResources);
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
    int memoryMbs = this.config.hasPath(ConfigurationConstants.APP_MASTER_MEMORY_MBS_KEY) ?
        this.config.getInt(ConfigurationConstants.APP_MASTER_MEMORY_MBS_KEY) :
        ConfigurationConstants.DEFAULT_APP_MASTER_MEMORY_MBS;
    int maximumMemoryCapacity = newApplicationResponse.getMaximumResourceCapability().getMemory();
    if (memoryMbs > maximumMemoryCapacity) {
      LOGGER.info(String.format("Specified AM memory [%d] is above the maximum memory capacity [%d] of the "
          + "cluster, using the maximum memory capacity instead.", memoryMbs, maximumMemoryCapacity));
      memoryMbs = maximumMemoryCapacity;
    }

    int vCores = this.config.hasPath(ConfigurationConstants.APP_MASTER_CORES_KEY) ? this.config
        .getInt(ConfigurationConstants.APP_MASTER_CORES_KEY) : ConfigurationConstants.DEFAULT_APP_MASTER_CORES;
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
    LOGGER.info(String.format("Created a LocalResource for file %s of type %s",
        fileResource.getResource(), resourceType));
    resourceMap.put(destFilePath.getName(), fileResource);
  }

  private Map<String, String> getEnvironmentVariables() {
    Map<String, String> environmentVariableMap = Maps.newHashMap();

    Apps.addToEnvironment(environmentVariableMap, ApplicationConstants.Environment.JAVA_HOME.key(),
        System.getenv(ApplicationConstants.Environment.JAVA_HOME.key()));

    // Add jars/files in the working directory of the ApplicationMaster to the CLASSPATH first
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
    return String.format("%s/bin/java -Xmx%dM %s" +
            " --%s %s" +
            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "%s.%s" +
            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separator + "%s.%s",
        ApplicationConstants.Environment.JAVA_HOME.$(), memoryMbs, GobblinApplicationMaster.class.getName(),
        ConfigurationConstants.APPLICATION_NAME_OPTION_NAME, this.appName, appMasterClassName,
        ApplicationConstants.STDOUT, appMasterClassName, ApplicationConstants.STDERR);
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

  private void cleanUpAppWorkDirectory(ApplicationId applicationId) throws IOException {
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.appName, applicationId);
    if (this.fs.exists(appWorkDir)) {
      LOGGER.info("Deleting application working directory " + appWorkDir);
      this.fs.delete(appWorkDir, true);
    }
  }

  public static void main(String[] args) throws Exception {
    Closer closer = Closer.create();
    try {
      final GobblinYarnAppLauncher gobblinYarnAppLauncher = closer.register(
          new GobblinYarnAppLauncher(ConfigFactory.load()));
      Runtime.getRuntime().addShutdownHook(new Thread() {

        @Override
        public void run() {
          try {
            gobblinYarnAppLauncher.close();
          } catch (IOException ioe) {
            LOGGER.error("Failed to shutdown the " + GobblinYarnAppLauncher.class.getSimpleName(), ioe);
          }
        }
      });

      gobblinYarnAppLauncher.launch();
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }
}
