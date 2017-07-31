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
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;

import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterMetricTagNames;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.yarn.event.ContainerShutdownRequest;
import org.apache.gobblin.yarn.event.NewContainerRequest;


/**
 * This class is responsible for all Yarn-related stuffs including ApplicationMaster registration,
 * ApplicationMaster un-registration, Yarn container management, etc.
 *
 * @author Yinan Li
 */
public class YarnService extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnService.class);

  private static final Splitter SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

  private final String applicationName;
  private final String applicationId;

  private final Config config;

  private final EventBus eventBus;

  private final Configuration yarnConfiguration;
  private final FileSystem fs;

  private final Optional<GobblinMetrics> gobblinMetrics;
  private final Optional<EventSubmitter> eventSubmitter;

  private final AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;
  private final NMClientAsync nmClientAsync;
  private final ExecutorService containerLaunchExecutor;

  private final int initialContainers;
  private final int requestedContainerMemoryMbs;
  private final int requestedContainerCores;
  private final boolean containerHostAffinityEnabled;

  private final int helixInstanceMaxRetries;

  private final Optional<String> containerJvmArgs;

  private volatile Optional<Resource> maxResourceCapacity = Optional.absent();

  // Security tokens for accessing HDFS
  private final ByteBuffer tokens;

  private final Closer closer = Closer.create();

  private final Object allContainersStopped = new Object();

  // A map from container IDs to pairs of Container instances and Helix participant IDs of the containers
  private final ConcurrentMap<ContainerId, Map.Entry<Container, String>> containerMap = Maps.newConcurrentMap();

  // A generator for an integer ID of a Helix instance (participant)
  private final AtomicInteger helixInstanceIdGenerator = new AtomicInteger(0);

  // A map from Helix instance names to the number times the instances are retried to be started
  private final ConcurrentMap<String, AtomicInteger> helixInstanceRetryCount = Maps.newConcurrentMap();

  // A queue of unused Helix instance names. An unused Helix instance name gets put
  // into the queue if the container running the instance completes. Unused Helix
  // instance names get picked up when replacement containers get allocated.
  private final ConcurrentLinkedQueue<String> unusedHelixInstanceNames = Queues.newConcurrentLinkedQueue();

  private volatile boolean shutdownInProgress = false;

  public YarnService(Config config, String applicationName, String applicationId, YarnConfiguration yarnConfiguration,
      FileSystem fs, EventBus eventBus) throws Exception {
    this.applicationName = applicationName;
    this.applicationId = applicationId;

    this.config = config;

    this.eventBus = eventBus;

    this.gobblinMetrics = config.getBoolean(ConfigurationKeys.METRICS_ENABLED_KEY) ?
        Optional.of(buildGobblinMetrics()) : Optional.<GobblinMetrics>absent();

    this.eventSubmitter = config.getBoolean(ConfigurationKeys.METRICS_ENABLED_KEY) ?
        Optional.of(buildEventSubmitter()) : Optional.<EventSubmitter>absent();

    this.yarnConfiguration = yarnConfiguration;
    this.fs = fs;

    this.amrmClientAsync = closer.register(
        AMRMClientAsync.createAMRMClientAsync(1000, new AMRMClientCallbackHandler()));
    this.amrmClientAsync.init(this.yarnConfiguration);
    this.nmClientAsync = closer.register(NMClientAsync.createNMClientAsync(new NMClientCallbackHandler()));
    this.nmClientAsync.init(this.yarnConfiguration);

    this.initialContainers = config.getInt(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY);
    this.requestedContainerMemoryMbs = config.getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    this.requestedContainerCores = config.getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY);
    this.containerHostAffinityEnabled = config.getBoolean(GobblinYarnConfigurationKeys.CONTAINER_HOST_AFFINITY_ENABLED);

    this.helixInstanceMaxRetries = config.getInt(GobblinYarnConfigurationKeys.HELIX_INSTANCE_MAX_RETRIES);

    this.containerJvmArgs = config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinYarnConfigurationKeys.CONTAINER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    this.containerLaunchExecutor = Executors.newFixedThreadPool(10,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ContainerLaunchExecutor")));

    this.tokens = getSecurityTokens();
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleNewContainerRequest(NewContainerRequest newContainerRequest) {
    if (!this.maxResourceCapacity.isPresent()) {
      LOGGER.error(String.format(
          "Unable to handle new container request as maximum resource capacity is not available: "
              + "[memory (MBs) requested = %d, vcores requested = %d]", this.requestedContainerMemoryMbs,
          this.requestedContainerCores));
      return;
    }

    requestContainer(newContainerRequest.getReplacedContainer().transform(new Function<Container, String>() {

      @Override
      public String apply(Container container) {
        return container.getNodeId().getHost();
      }
    }));
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

    // Register itself with the EventBus for container-related requests
    this.eventBus.register(this);

    this.amrmClientAsync.start();
    this.nmClientAsync.start();

    // The ApplicationMaster registration response is used to determine the maximum resource capacity of the cluster
    RegisterApplicationMasterResponse response = this.amrmClientAsync.registerApplicationMaster(
        GobblinClusterUtils.getHostname(), -1, "");
    LOGGER.info("ApplicationMaster registration response: " + response);
    this.maxResourceCapacity = Optional.of(response.getMaximumResourceCapability());

    LOGGER.info("Requesting initial containers");
    requestInitialContainers(this.initialContainers);
  }

  @Override
  protected void shutDown() throws IOException {
    LOGGER.info("Stopping the YarnService");

    this.shutdownInProgress = true;

    try {
      ExecutorsUtils.shutdownExecutorService(this.containerLaunchExecutor, Optional.of(LOGGER));

      // Stop the running containers
      for (Map.Entry<Container, String> entry : this.containerMap.values()) {
        LOGGER.info(String.format("Stopping container %s running participant %s", entry.getKey().getId(),
            entry.getValue()));
        this.nmClientAsync.stopContainerAsync(entry.getKey().getId(), entry.getKey().getNodeId());
      }

      if (!this.containerMap.isEmpty()) {
        synchronized (this.allContainersStopped) {
          try {
            // Wait 5 minutes for the containers to stop
            this.allContainersStopped.wait(5 * 60 * 1000);
            LOGGER.info("All of the containers have been stopped");
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }

      this.amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
    } catch (IOException | YarnException e) {
      LOGGER.error("Failed to unregister the ApplicationMaster", e);
    } finally {
      try {
        this.closer.close();
      } finally {
        if (this.gobblinMetrics.isPresent()) {
          this.gobblinMetrics.get().stopMetricsReporting();
        }
      }
    }
  }

  private GobblinMetrics buildGobblinMetrics() {
    // Create tags list
    ImmutableList.Builder<Tag<?>> tags = new ImmutableList.Builder<>();
    tags.add(new Tag<>(GobblinClusterMetricTagNames.APPLICATION_ID, this.applicationId));
    tags.add(new Tag<>(GobblinClusterMetricTagNames.APPLICATION_NAME, this.applicationName));

    // Intialize Gobblin metrics and start reporters
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(this.applicationId, null, tags.build());
    gobblinMetrics.startMetricReporting(ConfigUtils.configToProperties(config));

    return gobblinMetrics;
  }

  private EventSubmitter buildEventSubmitter() {
    return new EventSubmitter.Builder(this.gobblinMetrics.get().getMetricContext(),
        GobblinYarnEventConstants.EVENT_NAMESPACE)
        .build();
  }

  private void requestInitialContainers(int containersRequested) {
    for (int i = 0; i < containersRequested; i++) {
      requestContainer(Optional.<String>absent());
    }
  }

  private void requestContainer(Optional<String> preferredNode) {
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    Resource capability = Records.newRecord(Resource.class);
    int maxMemoryCapacity = this.maxResourceCapacity.get().getMemory();
    capability.setMemory(this.requestedContainerMemoryMbs <= maxMemoryCapacity ?
        this.requestedContainerMemoryMbs : maxMemoryCapacity);
    int maxCoreCapacity = this.maxResourceCapacity.get().getVirtualCores();
    capability.setVirtualCores(this.requestedContainerCores <= maxCoreCapacity ?
        this.requestedContainerCores : maxCoreCapacity);

    String[] preferredNodes = preferredNode.isPresent() ? new String[] {preferredNode.get()} : null;
    this.amrmClientAsync.addContainerRequest(
        new AMRMClient.ContainerRequest(capability, preferredNodes, null, priority));
  }

  private ContainerLaunchContext newContainerLaunchContext(Container container, String helixInstanceName)
      throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPath(this.fs, this.applicationName, this.applicationId);
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
    containerLaunchContext.setEnvironment(YarnHelixUtils.getEnvironmentVariables(this.yarnConfiguration));
    containerLaunchContext.setCommands(Lists.newArrayList(buildContainerCommand(container, helixInstanceName)));

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

  private String buildContainerCommand(Container container, String helixInstanceName) {
    String containerProcessName = GobblinYarnTaskRunner.class.getSimpleName();
    return new StringBuilder()
        .append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
        .append(" -Xmx").append(container.getResource().getMemory()).append("M")
        .append(" ").append(JvmUtils.formatJvmArguments(this.containerJvmArgs))
        .append(" ").append(GobblinYarnTaskRunner.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.applicationName)
        .append(" --").append(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)
        .append(" ").append(helixInstanceName)
        .append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
          containerProcessName).append(".").append(ApplicationConstants.STDOUT)
        .append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
          containerProcessName).append(".").append(ApplicationConstants.STDERR)
        .toString();
  }

  /**
   * Check the exit status of a completed container and see if the replacement container
   * should try to be started on the same node. Some exit status indicates a disk or
   * node failure and in such cases the replacement container should try to be started on
   * a different node.
   */
  private boolean shouldStickToTheSameNode(int containerExitStatus) {
    switch (containerExitStatus) {
      case ContainerExitStatus.DISKS_FAILED:
        return false;
      case ContainerExitStatus.ABORTED:
        // Mostly likely this exit status is due to node failures because the
        // application itself will not release containers.
        return false;
      default:
        // Stick to the same node for other cases if host affinity is enabled.
        return this.containerHostAffinityEnabled;
    }
  }

  /**
   * Handle the completion of a container. A new container will be requested to replace the one
   * that just exited. Depending on the exit status and if container host affinity is enabled,
   * the new container may or may not try to be started on the same node.
   *
   * A container completes in either of the following conditions: 1) some error happens in the
   * container and caused the container to exit, 2) the container gets killed due to some reason,
   * for example, if it runs over the allowed amount of virtual or physical memory, 3) the gets
   * preempted by the ResourceManager, or 4) the container gets stopped by the ApplicationMaster.
   * A replacement container is needed in all but the last case.
   */
  private void handleContainerCompletion(ContainerStatus containerStatus) {
    Map.Entry<Container, String> completedContainerEntry = this.containerMap.remove(containerStatus.getContainerId());
    String completedInstanceName = completedContainerEntry.getValue();

    LOGGER.info(String.format("Container %s running Helix instance %s has completed with exit status %d",
        containerStatus.getContainerId(), completedInstanceName, containerStatus.getExitStatus()));

    if (!Strings.isNullOrEmpty(containerStatus.getDiagnostics())) {
      LOGGER.info(String.format("Received the following diagnostics information for container %s: %s",
          containerStatus.getContainerId(), containerStatus.getDiagnostics()));
    }

    if (this.shutdownInProgress) {
      return;
    }

    this.helixInstanceRetryCount.putIfAbsent(completedInstanceName, new AtomicInteger(0));
    int retryCount =
    	 this.helixInstanceRetryCount.get(completedInstanceName).incrementAndGet();

    // Populate event metadata
    Optional<ImmutableMap.Builder<String, String>> eventMetadataBuilder = Optional.absent();
    if (this.eventSubmitter.isPresent()) {
      eventMetadataBuilder = Optional.of(buildContainerStatusEventMetadata(containerStatus));
      eventMetadataBuilder.get().put(GobblinYarnEventConstants.EventMetadata.HELIX_INSTANCE_ID, completedInstanceName);
      eventMetadataBuilder.get().put(GobblinYarnEventConstants.EventMetadata.CONTAINER_STATUS_RETRY_ATTEMPT, retryCount + "");
    }

    if (this.helixInstanceMaxRetries > 0 && retryCount > this.helixInstanceMaxRetries) {
      if (this.eventSubmitter.isPresent()) {
        this.eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.HELIX_INSTANCE_COMPLETION,
            eventMetadataBuilder.get().build());
      }

      LOGGER.warn("Maximum number of retries has been achieved for Helix instance " + completedInstanceName);
      return;
    }

    // Add the Helix instance name of the completed container to the queue of unused
    // instance names so they can be reused by a replacement container.
    this.unusedHelixInstanceNames.offer(completedInstanceName);

    if (this.eventSubmitter.isPresent()) {
      this.eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.HELIX_INSTANCE_COMPLETION,
          eventMetadataBuilder.get().build());
    }

    LOGGER.info(String.format("Requesting a new container to replace %s to run Helix instance %s",
        containerStatus.getContainerId(), completedInstanceName));
    this.eventBus.post(new NewContainerRequest(
        shouldStickToTheSameNode(containerStatus.getExitStatus()) ?
            Optional.of(completedContainerEntry.getKey()) : Optional.<Container>absent()));
  }

  private ImmutableMap.Builder<String, String> buildContainerStatusEventMetadata(ContainerStatus containerStatus) {
    ImmutableMap.Builder<String, String> eventMetadataBuilder = new ImmutableMap.Builder<>();
    eventMetadataBuilder.put(GobblinYarnMetricTagNames.CONTAINER_ID, containerStatus.getContainerId().toString());
    eventMetadataBuilder.put(GobblinYarnEventConstants.EventMetadata.CONTAINER_STATUS_CONTAINER_STATE,
        containerStatus.getState().toString());
    if (ContainerExitStatus.INVALID != containerStatus.getExitStatus()) {
      eventMetadataBuilder.put(GobblinYarnEventConstants.EventMetadata.CONTAINER_STATUS_EXIT_STATUS,
          containerStatus.getExitStatus() + "");
    }
    if (!Strings.isNullOrEmpty(containerStatus.getDiagnostics())) {
      eventMetadataBuilder.put(GobblinYarnEventConstants.EventMetadata.CONTAINER_STATUS_EXIT_DIAGNOSTICS,
          containerStatus.getDiagnostics());
    }

    return eventMetadataBuilder;
  }

  /**
   * A custom implementation of {@link AMRMClientAsync.CallbackHandler}.
   */
  private class AMRMClientCallbackHandler implements AMRMClientAsync.CallbackHandler {

    private volatile boolean done = false;

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for (ContainerStatus containerStatus : statuses) {
        handleContainerCompletion(containerStatus);
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      for (final Container container : containers) {
        if (eventSubmitter.isPresent()) {
          eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_ALLOCATION,
              GobblinYarnMetricTagNames.CONTAINER_ID, container.getId().toString());
        }

        LOGGER.info(String.format("Container %s has been allocated", container.getId()));

        String instanceName = unusedHelixInstanceNames.poll();
        if (Strings.isNullOrEmpty(instanceName)) {
          // No unused instance name, so generating a new one.
          instanceName = HelixUtils.getHelixInstanceName(GobblinYarnTaskRunner.class.getSimpleName(),
              helixInstanceIdGenerator.incrementAndGet());
        }

        final String finalInstanceName = instanceName;
        containerMap.put(container.getId(), new AbstractMap.SimpleImmutableEntry<>(container, finalInstanceName));

        containerLaunchExecutor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              LOGGER.info("Starting container " + container.getId());

              nmClientAsync.startContainerAsync(container, newContainerLaunchContext(container, finalInstanceName));
            } catch (IOException ioe) {
              LOGGER.error("Failed to start container " + container.getId(), ioe);
            }
          }
        });
      }
    }

    @Override
    public void onShutdownRequest() {
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.SHUTDOWN_REQUEST);
      }

      LOGGER.info("Received shutdown request from the ResourceManager");
      this.done = true;
      eventBus.post(new ClusterManagerShutdownRequest());
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
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.ERROR,
            GobblinYarnEventConstants.EventMetadata.ERROR_EXCEPTION, Throwables.getStackTraceAsString(t));
      }

      LOGGER.error("Received error: " + t, t);
      this.done = true;
      eventBus.post(new ClusterManagerShutdownRequest());
    }
  }

  /**
   * A custom implementation of {@link NMClientAsync.CallbackHandler}.
   */
  private class NMClientCallbackHandler implements NMClientAsync.CallbackHandler {

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_STARTED,
            GobblinYarnMetricTagNames.CONTAINER_ID, containerId.toString());
      }

      LOGGER.info(String.format("Container %s has been started", containerId));
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_STATUS_RECEIVED,
            buildContainerStatusEventMetadata(containerStatus).build());
      }

      LOGGER.info(String.format("Received container status for container %s: %s", containerId, containerStatus));
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_STOPPED,
            GobblinYarnMetricTagNames.CONTAINER_ID, containerId.toString());
      }

      LOGGER.info(String.format("Container %s has been stopped", containerId));
      containerMap.remove(containerId);
      if (containerMap.isEmpty()) {
        synchronized (allContainersStopped) {
          allContainersStopped.notify();
        }
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_START_ERROR,
            GobblinYarnMetricTagNames.CONTAINER_ID, containerId.toString(),
            GobblinYarnEventConstants.EventMetadata.ERROR_EXCEPTION, Throwables.getStackTraceAsString(t));
      }

      LOGGER.error(String.format("Failed to start container %s due to error %s", containerId, t));
      containerMap.remove(containerId);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_GET_STATUS_ERROR,
            GobblinYarnMetricTagNames.CONTAINER_ID, containerId.toString(),
            GobblinYarnEventConstants.EventMetadata.ERROR_EXCEPTION, Throwables.getStackTraceAsString(t));
      }

      LOGGER.error(String.format("Failed to get status for container %s due to error %s", containerId, t));
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      if (eventSubmitter.isPresent()) {
        eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_STOP_ERROR,
            GobblinYarnMetricTagNames.CONTAINER_ID, containerId.toString(),
            GobblinYarnEventConstants.EventMetadata.ERROR_EXCEPTION, Throwables.getStackTraceAsString(t));
      }

      LOGGER.error(String.format("Failed to stop container %s due to error %s", containerId, t));
    }
  }
}
