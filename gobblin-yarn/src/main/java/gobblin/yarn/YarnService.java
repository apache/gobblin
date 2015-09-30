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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.api.id.ParticipantId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;

import com.typesafe.config.Config;

import gobblin.util.ExecutorsUtils;
import gobblin.yarn.event.ApplicationMasterShutdownRequest;
import gobblin.yarn.event.ContainerShutdownRequest;
import gobblin.yarn.event.NewContainerRequest;


/**
 * This class is responsible for all Yarn-related stuffs including ApplicationMaster registration,
 * ApplicationMaster un-registration, Yarn container management, etc.
 *
 * @author ynli
 */
public class YarnService extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnService.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final ConcurrentMap<ContainerId, Container> containerMap = Maps.newConcurrentMap();
  private final ConcurrentMap<ContainerId, ParticipantId> containerToParticipantMap = Maps.newConcurrentMap();

  private final ApplicationId applicationId;
  private final String applicationName;

  private final Config config;

  private final Configuration yarnConfiguration;
  private final AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;
  private final NMClientAsync nmClientAsync;
  private final ExecutorService containerLaunchExecutor;
  private final FileSystem fs;

  private final EventBus eventBus;

  private final int initialContainers;
  private final int requestedContainerMemoryMbs;
  private final int requestedContainerCores;

  private final String containerJvmArgs;

  // Security tokens for accessing HDFS
  private final ByteBuffer tokens;

  private final Closer closer = Closer.create();

  private final Object allContainersStopped = new Object();

  private volatile Optional<Resource> maxResourceCapacity =Optional.absent();

  public YarnService(Config config, String applicationName, ApplicationId applicationId, FileSystem fs,
      EventBus eventBus, String containerJvmArgs) throws Exception {
    this.applicationName = applicationName;
    this.applicationId = applicationId;

    this.config = config;

    this.yarnConfiguration = new YarnConfiguration();
    this.amrmClientAsync = closer.register(
        AMRMClientAsync.createAMRMClientAsync(1000, new AMRMClientCallbackHandler()));
    this.amrmClientAsync.init(this.yarnConfiguration);
    this.nmClientAsync = closer.register(NMClientAsync.createNMClientAsync(new NMClientCallbackHandler()));
    this.nmClientAsync.init(this.yarnConfiguration);

    this.containerLaunchExecutor = Executors.newFixedThreadPool(10,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ContainerLaunchExecutor")));

    this.fs = fs;

    this.eventBus = eventBus;

    this.initialContainers = config.getInt(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY);
    this.requestedContainerMemoryMbs = config.getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    this.requestedContainerCores = config.getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY);

    this.containerJvmArgs = containerJvmArgs;

    this.tokens = getSecurityTokens();

    // Register itself with the EventBus for container-related requests
    this.eventBus.register(this);
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleNewContainerRequest(NewContainerRequest newContainerRequest) {
    int newContainersRequested = newContainerRequest.getNewContainersRequested();
    if (newContainersRequested <= 0) {
      LOGGER.error("Invalid number of new containers requested: " + newContainersRequested);
      return;
    }

    if (!this.maxResourceCapacity.isPresent()) {
      LOGGER.error("Unable to handle new container request as maximum resource capacity is not available");
      return;
    }

    try {
      requestContainers(newContainersRequested);
    } catch (IOException ioe) {
      LOGGER.error("Failed to handle new container request", ioe);
    } catch (YarnException ye) {
      LOGGER.error("Failed to handle new container request", ye);
    }
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleContainerShutdownRequest(ContainerShutdownRequest containerShutdownRequest) {
    for (Container container : containerShutdownRequest.getContainers()) {
      LOGGER.info(String.format("Stopping container %s running on %s", container.getId(), container.getNodeId()));
      this.nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOGGER.info("Starting the YarnService");
    this.amrmClientAsync.start();
    this.nmClientAsync.start();

    // The ApplicationMaster registration response is used to determine the maximum resource capacity of the cluster
    RegisterApplicationMasterResponse response = this.amrmClientAsync.registerApplicationMaster(
        YarnHelixUtils.getHostname(), -1, "");
    LOGGER.info("ApplicationMaster registration response: " + response);
    this.maxResourceCapacity = Optional.of(response.getMaximumResourceCapability());

    LOGGER.info("Requesting initial containers");
    requestContainers(this.initialContainers);
  }

  @Override
  protected void shutDown() throws Exception {
    LOGGER.info("Stopping the YarnService");

    try {
      ExecutorsUtils.shutdownExecutorService(this.containerLaunchExecutor, Optional.of(LOGGER));

      // Stop the running containers
      for (Container container : this.containerMap.values()) {
        LOGGER.info("Stopping container " + container.getId());
        this.nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
      }

      if (!this.containerMap.isEmpty()) {
        synchronized (this.allContainersStopped) {
          // Wait 5 minutes for the containers to stop
          this.allContainersStopped.wait(5 * 60 * 1000);
          LOGGER.info("All of the containers have been stopped");
        }
      }

      this.amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
    } catch (IOException ioe) {
      LOGGER.error("Failed to unregister the ApplicationMaster", ioe);
    } catch (YarnException ye) {
      LOGGER.error("Failed to unregister the ApplicationMaster", ye);
    } catch (Exception e) {
      LOGGER.error("Failed to unregister the ApplicationMaster", e);
    } finally {
      this.closer.close();
    }
  }

  private void requestContainers(int containersRequested) throws IOException, YarnException {
    for (int i = 0; i < containersRequested; i++) {
      Priority priority = Records.newRecord(Priority.class);
      priority.setPriority(0);

      Resource capability = Records.newRecord(Resource.class);
      int maxMemoryCapacity = this.maxResourceCapacity.get().getMemory();
      capability.setMemory(this.requestedContainerMemoryMbs <= maxMemoryCapacity ?
          this.requestedContainerMemoryMbs : maxMemoryCapacity);
      int maxCoreCapacity = this.maxResourceCapacity.get().getVirtualCores();
      capability.setVirtualCores(this.requestedContainerCores <= maxCoreCapacity ?
          this.requestedContainerCores : maxCoreCapacity);

      this.amrmClientAsync.addContainerRequest(new AMRMClient.ContainerRequest(capability, null, null, priority));
    }
  }

  private ContainerLaunchContext newContainerLaunchContext(Container container) throws IOException {
    Path appWorkDir = YarnHelixUtils.getAppWorkDirPath(this.fs, this.applicationName, this.applicationId);
    Path containerWorkDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);

    Map<String, LocalResource> resourceMap = Maps.newHashMap();

    addContainerLocalResources(new Path(appWorkDir, GobblinYarnConfigurationKeys.LIB_JARS_DIR_NAME), resourceMap);
    addContainerLocalResources(new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME), resourceMap);
    addContainerLocalResources(
        new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME), resourceMap);

    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_FILES_REMOTE_KEY)) {
      addRemoteAppFiles(this.config.getString(GobblinYarnConfigurationKeys.CONTAINER_FILES_REMOTE_KEY), resourceMap);
    }

    ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    containerLaunchContext.setLocalResources(resourceMap);
    containerLaunchContext.setEnvironment(YarnHelixUtils.getEnvironmentVariables(this.yarnConfiguration, this.config));
    containerLaunchContext.setCommands(Lists.newArrayList(buildContainerCommand(container)));

    if (UserGroupInformation.isSecurityEnabled()) {
      containerLaunchContext.setTokens(this.tokens.duplicate());
    }

    return containerLaunchContext;
  }

  private void addContainerLocalResources(Path destDir, Map<String, LocalResource> resourceMap) throws IOException {
    if (!this.fs.exists(destDir)) {
      LOGGER.warn(String.format("Path %s does not exist so no container LocalResource to add", destDir));
      return;
    }

    FileStatus[] statuses = this.fs.listStatus(destDir);
    if (statuses != null) {
      for (FileStatus status : statuses) {
        YarnHelixUtils.addFileAsLocalResource(this.fs, status.getPath(), LocalResourceType.FILE, resourceMap);
      }
    }
  }

  private void addRemoteAppFiles(String hdfsFileList, Map<String, LocalResource> resourceMap) throws IOException {
    for (String hdfsFilePath : SPLITTER.split(hdfsFileList)) {
      Path srcFilePath = new Path(hdfsFilePath);
      YarnHelixUtils.addFileAsLocalResource(
          srcFilePath.getFileSystem(this.yarnConfiguration), srcFilePath, LocalResourceType.FILE, resourceMap);
    }
  }

  private ByteBuffer getSecurityTokens() throws IOException {
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    Closer closer = Closer.create();
    try {
      DataOutputBuffer dataOutputBuffer = closer.register(new DataOutputBuffer());
      credentials.writeTokenStorageToStream(dataOutputBuffer);

      // Remove the AM->RM token so that containers cannot access it
      Iterator<Token<?>> tokenIterator = credentials.getAllTokens().iterator();
      while (tokenIterator.hasNext()) {
        Token<?> token = tokenIterator.next();
        if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
          tokenIterator.remove();
        }
      }

      return ByteBuffer.wrap(dataOutputBuffer.getData(), 0, dataOutputBuffer.getLength());
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  private String buildContainerCommand(Container container) {
    String containerProcessName = GobblinWorkUnitRunner.class.getSimpleName();
    return new StringBuilder()
        .append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
        .append(" -Xmx").append(container.getResource().getMemory()).append("M")
        .append(" ").append(this.containerJvmArgs)
        .append(" ").append(GobblinWorkUnitRunner.class.getName())
        .append(" --").append(GobblinYarnConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.applicationName)
        .append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
          containerProcessName).append(".").append(ApplicationConstants.STDOUT)
        .append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
          containerProcessName).append(".").append(ApplicationConstants.STDERR)
        .toString();
  }

  /**
   * A custom implementation of {@link AMRMClientAsync.CallbackHandler}.
   */
  private class AMRMClientCallbackHandler implements AMRMClientAsync.CallbackHandler {

    private volatile boolean done = false;

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for (ContainerStatus containerStatus : statuses) {
        LOGGER.info(String.format("Container %s has completed with exit status %d",
            containerStatus.getContainerId(), containerStatus.getExitStatus()));
        containerMap.remove(containerStatus.getContainerId());
        containerToParticipantMap.remove(containerStatus.getContainerId());
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      for (final Container container : containers) {
        LOGGER.info(String.format("Container %s has been allocated", container.getId()));
        containerMap.put(container.getId(), container);
        containerToParticipantMap.put(container.getId(),
            YarnHelixUtils.getParticipantId(container.getNodeId().getHost(), container.getId()));
        containerLaunchExecutor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              LOGGER.info("Starting container " + container.getId());
              nmClientAsync.startContainerAsync(container, newContainerLaunchContext(container));
            } catch (IOException ioe) {
              LOGGER.error("Failed to start container " + container.getId(), ioe);
            }
          }
        });
      }
    }

    @Override
    public void onShutdownRequest() {
      LOGGER.info("Received shutdown request from the ResourceManager");
      this.done = true;
      eventBus.post(new ApplicationMasterShutdownRequest());
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
      for (NodeReport nodeReport : updatedNodes) {
        LOGGER.info("Received node update report: " + nodeReport);
      }
    }

    @Override
    public float getProgress() {
      return this.done ? 1.0f : 0.0f;
    }

    @Override
    public void onError(Throwable t) {
      LOGGER.error("Received error: " + t, t);
      this.done = true;
      eventBus.post(new ApplicationMasterShutdownRequest());
    }
  }

  /**
   * A custom implementation of {@link NMClientAsync.CallbackHandler}.
   */
  private class NMClientCallbackHandler implements NMClientAsync.CallbackHandler {

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      LOGGER.info(String.format("Container %s has been started", containerId));
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOGGER.info(String.format("Received container status for container %s: %s", containerId, containerStatus));
      if (containerStatus.getState() == ContainerState.COMPLETE) {
        LOGGER.info(String.format("Container %s completed", containerId));
        containerMap.remove(containerId);
        containerToParticipantMap.remove(containerId);
        if (containerMap.isEmpty()) {
          synchronized (allContainersStopped) {
            allContainersStopped.notify();
          }
        }
      }
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      LOGGER.info(String.format("Container %s has been stopped", containerId));
      containerMap.remove(containerId);
      containerToParticipantMap.remove(containerId);
      if (containerMap.isEmpty()) {
        synchronized (allContainersStopped) {
          allContainersStopped.notify();
        }
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOGGER.error(String.format("Failed to start container %s due to error %s", containerId, t));
      containerMap.remove(containerId);
      containerToParticipantMap.remove(containerId);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOGGER.error(String.format("Failed to get status for container %s due to error %s", containerId, t));
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOGGER.error(String.format("Failed to stop container %s due to error %s", containerId, t));
    }
  }
}
