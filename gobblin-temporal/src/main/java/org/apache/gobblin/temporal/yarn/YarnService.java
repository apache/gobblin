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

package org.apache.gobblin.temporal.yarn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
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
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import lombok.AccessLevel;
import lombok.Getter;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterMetricTagNames;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricReporterException;
import org.apache.gobblin.metrics.MultiReporterException;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.executors.ScalingThreadPoolExecutor;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;
import org.apache.gobblin.yarn.GobblinYarnEventConstants;
import org.apache.gobblin.yarn.GobblinYarnMetricTagNames;
import org.apache.gobblin.yarn.YarnHelixUtils;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.WorkerProfile;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;

/**
 * This class is responsible for all Yarn-related stuffs including ApplicationMaster registration,
 * ApplicationMaster un-registration, Yarn container management, etc.
 *
 * NOTE: This is a stripped down version of {@link org.apache.gobblin.yarn.YarnService} that is used for temporal testing
 * without any dependency on Helix. There are some references to helix concepts, but they are left in for the sake of
 * keeping some features in-tact. They don't have an actual dependency on helix anymore.
 *
 */
class YarnService extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnService.class);

  private final String applicationName;
  private final String applicationId;
  private final String appViewAcl;
  protected final Config config;

  private final EventBus eventBus;

  private final Configuration yarnConfiguration;
  private final FileSystem fs;

  private final Optional<GobblinMetrics> gobblinMetrics;
  private final Optional<EventSubmitter> eventSubmitter;

  @Getter(AccessLevel.PROTECTED)
  private final AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;
  private final NMClientAsync nmClientAsync;
  private final ExecutorService containerLaunchExecutor;
  private final String containerTimezone;
  private final String proxyJvmArgs;

  @Getter(AccessLevel.PROTECTED)
  private volatile Optional<Resource> maxResourceCapacity = Optional.absent();

  // Security tokens for accessing HDFS
  private ByteBuffer tokens;

  private final Closer closer = Closer.create();

  private final Object allContainersStopped = new Object();

  // A map from container IDs to Container instances, WorkerProfile Name and WorkerProfile Object
  protected final ConcurrentMap<ContainerId, ContainerInfo> containerMap = new ConcurrentHashMap<>();

  // A cache of the containers with an outstanding container release request.
  // This is a cache instead of a set to get the automatic cleanup in case a container completes before the requested
  // release.
  protected final Cache<ContainerId, String> releasedContainerCache;

  private final AtomicInteger priorityNumGenerator = new AtomicInteger(0);
  private final Map<String, Integer> resourcePriorityMap = new HashMap<>();

  protected volatile boolean shutdownInProgress = false;

  private final boolean jarCacheEnabled;
  private static final long DEFAULT_ALLOCATION_REQUEST_ID = 0L;
  private final AtomicLong allocationRequestIdGenerator = new AtomicLong(DEFAULT_ALLOCATION_REQUEST_ID);
  private final ConcurrentMap<Long, WorkerProfile> workerProfileByAllocationRequestId = new ConcurrentHashMap<>();

  public YarnService(Config config, String applicationName, String applicationId, YarnConfiguration yarnConfiguration,
      FileSystem fs, EventBus eventBus) throws Exception {
    this.applicationName = applicationName;
    this.applicationId = applicationId;

    this.config = config;

    this.eventBus = eventBus;

    // Gobblin metrics have been disabled to allow testing kafka integration without having to setup
    // the metrics reporting topic. For this Temporal based impl of the Cluster Manager,
    // Kafka will only be used for GobblinTrackingEvents for external monitoring. This choice was made because metrics
    // emitted MetricReport are non-critical and not needed for the cluster / job execution correctness
    this.gobblinMetrics = Optional.<GobblinMetrics>absent();
    this.eventSubmitter = Optional.<EventSubmitter>absent();

    this.yarnConfiguration = yarnConfiguration;
    this.fs = fs;

    int amRmHeartbeatIntervalMillis = Long.valueOf(TimeUnit.SECONDS.toMillis(
        ConfigUtils.getInt(config, GobblinYarnConfigurationKeys.AMRM_HEARTBEAT_INTERVAL_SECS,
            GobblinYarnConfigurationKeys.DEFAULT_AMRM_HEARTBEAT_INTERVAL_SECS))).intValue();
    this.amrmClientAsync = closer.register(
        AMRMClientAsync.createAMRMClientAsync(amRmHeartbeatIntervalMillis, new AMRMClientCallbackHandler()));
    this.amrmClientAsync.init(this.yarnConfiguration);
    this.nmClientAsync = closer.register(NMClientAsync.createNMClientAsync(getNMClientCallbackHandler()));
    this.nmClientAsync.init(this.yarnConfiguration);

    this.proxyJvmArgs = config.hasPath(GobblinYarnConfigurationKeys.YARN_APPLICATION_PROXY_JVM_ARGS) ?
        config.getString(GobblinYarnConfigurationKeys.YARN_APPLICATION_PROXY_JVM_ARGS) : StringUtils.EMPTY;

    int numContainerLaunchThreads =
        ConfigUtils.getInt(config, GobblinYarnConfigurationKeys.MAX_CONTAINER_LAUNCH_THREADS_KEY,
            GobblinYarnConfigurationKeys.DEFAULT_MAX_CONTAINER_LAUNCH_THREADS);
    this.containerLaunchExecutor = ScalingThreadPoolExecutor.newScalingThreadPool(5, numContainerLaunchThreads, 0L,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ContainerLaunchExecutor")));

    this.tokens = getSecurityTokens();

    this.releasedContainerCache = CacheBuilder.newBuilder().expireAfterAccess(ConfigUtils.getInt(config,
        GobblinYarnConfigurationKeys.RELEASED_CONTAINERS_CACHE_EXPIRY_SECS,
        GobblinYarnConfigurationKeys.DEFAULT_RELEASED_CONTAINERS_CACHE_EXPIRY_SECS), TimeUnit.SECONDS).build();

    this.appViewAcl = ConfigUtils.getString(this.config, GobblinYarnConfigurationKeys.APP_VIEW_ACL,
        GobblinYarnConfigurationKeys.DEFAULT_APP_VIEW_ACL);
    this.containerTimezone = ConfigUtils.getString(this.config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_TIMEZONE,
        GobblinYarnConfigurationKeys.DEFAULT_GOBBLIN_YARN_CONTAINER_TIMEZONE);
    this.jarCacheEnabled = ConfigUtils.getBoolean(this.config, GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED, GobblinYarnConfigurationKeys.JAR_CACHE_ENABLED_DEFAULT);

  }

  protected NMClientCallbackHandler getNMClientCallbackHandler() {
    return new NMClientCallbackHandler();
  }

  @Override
  protected synchronized void startUp() throws Exception {
    LOGGER.info("Starting the TemporalYarnService");

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
    requestInitialContainers();
  }

  @Override
  protected void shutDown() throws IOException {
    LOGGER.info("Stopping the TemporalYarnService");

    this.shutdownInProgress = true;

    try {
      ExecutorsUtils.shutdownExecutorService(this.containerLaunchExecutor, Optional.of(LOGGER));

      // Stop the running containers
      for (ContainerInfo containerInfo : this.containerMap.values()) {
        LOGGER.info("Stopping container {} running worker profile {}", containerInfo.getContainer().getId(),
            containerInfo.getWorkerProfileName());
        this.nmClientAsync.stopContainerAsync(containerInfo.getContainer().getId(), containerInfo.getContainer().getNodeId());
      }

      if (!this.containerMap.isEmpty()) {
        synchronized (this.allContainersStopped) {
          try {
            // Wait 5 minutes for the containers to stop
            Duration waitTimeout = Duration.ofMinutes(5);
            this.allContainersStopped.wait(waitTimeout.toMillis());
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

  public void updateToken() throws IOException{
    this.tokens = getSecurityTokens();
  }

  private GobblinMetrics buildGobblinMetrics() {
    // Create tags list
    ImmutableList.Builder<Tag<?>> tags = new ImmutableList.Builder<>();
    tags.add(new Tag<>(GobblinClusterMetricTagNames.APPLICATION_ID, this.applicationId));
    tags.add(new Tag<>(GobblinClusterMetricTagNames.APPLICATION_NAME, this.applicationName));

    // Intialize Gobblin metrics and start reporters
    GobblinMetrics gobblinMetrics = GobblinMetrics.get(this.applicationId, null, tags.build());
    try {
      gobblinMetrics.startMetricReporting(ConfigUtils.configToProperties(config));
    } catch (MultiReporterException ex) {
      for (MetricReporterException e: ex.getExceptions()) {
        LOGGER.error("Failed to start {} {} reporter.", e.getSinkType().name(), e.getReporterType().name(), e);
      }
    }

    return gobblinMetrics;
  }

  private EventSubmitter buildEventSubmitter() {
    return new EventSubmitter.Builder(this.gobblinMetrics.get().getMetricContext(),
        GobblinYarnEventConstants.EVENT_NAMESPACE)
        .build();
  }

  /** unless overridden to actually scale, "initial" containers will be the app's *only* containers! */
  protected synchronized void requestInitialContainers() {
    WorkerProfile baselineWorkerProfile = new WorkerProfile(this.config);
    int numContainers = this.config.getInt(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY);
    LOGGER.info("Requesting {} initial (static) containers with baseline (only) profile, never to be re-scaled", numContainers);
    requestContainersForWorkerProfile(baselineWorkerProfile, numContainers);
  }

  protected synchronized void requestContainersForWorkerProfile(WorkerProfile workerProfile, int numContainers) {
    int containerMemoryMbs = workerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    int containerCores = workerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY);
    long allocationRequestId = storeByUniqueAllocationRequestId(workerProfile);
    requestContainers(numContainers, Resource.newInstance(containerMemoryMbs, containerCores), Optional.of(allocationRequestId));
  }

  protected synchronized void releaseContainersForWorkerProfile(String profileName, int numContainers) {
    int numContainersToRelease = numContainers;
    Iterator<Map.Entry<ContainerId, ContainerInfo>> containerMapIterator = this.containerMap.entrySet().iterator();
    while (containerMapIterator.hasNext() && numContainers > 0) {
      Map.Entry<ContainerId, ContainerInfo> entry = containerMapIterator.next();
      if (entry.getValue().getWorkerProfile().getName().equals(profileName)) {
        ContainerId containerId = entry.getKey();
        LOGGER.info("Releasing container {} running profile {}", containerId, WorkforceProfiles.renderName(profileName));
        // Record that this container was explicitly released so that a new one is not spawned to replace it
        // Put the container id in the releasedContainerCache before releasing it so that handleContainerCompletion()
        // can check for the container id and skip spawning a replacement container.
        // Note that this is the best effort since these are asynchronous operations and a container may abort concurrently
        // with the release call. So in some cases a replacement container may have already been spawned before
        // the container is put into the black list.
        this.releasedContainerCache.put(containerId, "");
        this.amrmClientAsync.releaseAssignedContainer(containerId);
        numContainers--;
      }
    }
    LOGGER.info("Released {} containers out of {} requested for profile {}", numContainersToRelease - numContainers,
        numContainersToRelease, profileName);
  }

  /**
   * Request {@param numContainers} from yarn with the specified resource. Resources will be allocated without a preferred
   * node
   * @param numContainers
   * @param resource
   */
  protected void requestContainers(int numContainers, Resource resource, Optional<Long> optAllocationRequestId) {
    LOGGER.info("Requesting {} containers with resource = {} and allocation request id = {}", numContainers, resource, optAllocationRequestId);
    IntStream.range(0, numContainers)
        .forEach(i -> requestContainer(Optional.absent(), resource, optAllocationRequestId));
  }

  // Request containers with specific resource requirement
  private void requestContainer(Optional<String> preferredNode, Resource resource, Optional<Long> optAllocationRequestId) {
    // Fail if Yarn cannot meet container resource requirements
    Preconditions.checkArgument(resource.getMemory() <= this.maxResourceCapacity.get().getMemory() &&
            resource.getVirtualCores() <= this.maxResourceCapacity.get().getVirtualCores(),
        "Resource requirement must less than the max resource capacity. Requested resource" + resource.toString()
            + " exceed the max resource limit " + this.maxResourceCapacity.get().toString());

    // Due to YARN-314, different resource capacity needs different priority, otherwise Yarn will not allocate container
    Priority priority = Records.newRecord(Priority.class);
    if(!resourcePriorityMap.containsKey(resource.toString())) {
      resourcePriorityMap.put(resource.toString(), priorityNumGenerator.getAndIncrement());
    }
    int priorityNum = resourcePriorityMap.get(resource.toString());
    priority.setPriority(priorityNum);

    String[] preferredNodes = preferredNode.isPresent() ? new String[] {preferredNode.get()} : null;

    long allocationRequestId = optAllocationRequestId.or(DEFAULT_ALLOCATION_REQUEST_ID);

    this.amrmClientAsync.addContainerRequest(
        new AMRMClient.ContainerRequest(resource, preferredNodes, null, priority, allocationRequestId));
  }

  protected ContainerLaunchContext newContainerLaunchContext(ContainerInfo containerInfo)
      throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, this.fs, this.applicationName, this.applicationId);
    // Used for -SNAPSHOT versions of jars
    Path containerJarsUnsharedDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);
    Path jarCacheDir = this.jarCacheEnabled ? YarnHelixUtils.calculatePerMonthJarCachePath(this.config) : appWorkDir;
    Path containerJarsCachedDir = new Path(jarCacheDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);
    LOGGER.info("Container cached jars root dir: " + containerJarsCachedDir);
    LOGGER.info("Container execution-private jars root dir: " + containerJarsUnsharedDir);
    Path containerWorkDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);


    Map<String, LocalResource> resourceMap = Maps.newHashMap();
    // Always fetch any jars from the appWorkDir for any potential snapshot jars
    addContainerLocalResources(new Path(appWorkDir, GobblinYarnConfigurationKeys.LIB_JARS_DIR_NAME), resourceMap);
    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY)) {
      addContainerLocalResources(new Path(containerJarsUnsharedDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME),
          resourceMap);
    }
    if (this.jarCacheEnabled) {
      addContainerLocalResources(new Path(jarCacheDir, GobblinYarnConfigurationKeys.LIB_JARS_DIR_NAME), resourceMap);
      if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_JARS_KEY)) {
        addContainerLocalResources(new Path(containerJarsCachedDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME),
            resourceMap);
      }
    }

    addContainerLocalResources(
        new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_FILES_DIR_NAME), resourceMap);

    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_FILES_REMOTE_KEY)) {
      YarnHelixUtils.addRemoteFilesToLocalResources(this.config.getString(GobblinYarnConfigurationKeys.CONTAINER_FILES_REMOTE_KEY),
          resourceMap, yarnConfiguration);
    }
    if (this.config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_ZIPS_REMOTE_KEY)) {
      YarnHelixUtils.addRemoteZipsToLocalResources(this.config.getString(GobblinYarnConfigurationKeys.CONTAINER_ZIPS_REMOTE_KEY),
          resourceMap, yarnConfiguration);
    }
    ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    containerLaunchContext.setLocalResources(resourceMap);
    containerLaunchContext.setEnvironment(YarnHelixUtils.getEnvironmentVariables(this.yarnConfiguration));
    containerLaunchContext.setCommands(Arrays.asList(containerInfo.getStartupCommand()));

    Map<ApplicationAccessType, String> acls = new HashMap<>(1);
    acls.put(ApplicationAccessType.VIEW_APP, this.appViewAcl);
    containerLaunchContext.setApplicationACLs(acls);

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
      Set<String> appLibJars = YarnHelixUtils.getAppLibJarList(this.config);
      for (FileStatus status : statuses) {
        String fileName = status.getPath().getName();
        // Ensure that we are only adding jars that were uploaded by the YarnAppLauncher for this application
        if (fileName.contains(".jar") && !appLibJars.contains(fileName)) {
          continue;
        }
        YarnHelixUtils.addFileAsLocalResource(this.fs, status.getPath(), LocalResourceType.FILE, resourceMap);
      }
    }
  }


  protected ByteBuffer getSecurityTokens() throws IOException {
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

  private String buildContainerCommand(Container container, String workerProfileName, WorkerProfile workerProfile) {
    Config workerProfileConfig = workerProfile.getConfig();

    double workerJvmMemoryXmxRatio = ConfigUtils.getDouble(workerProfileConfig,
        GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY,
        GobblinYarnConfigurationKeys.DEFAULT_CONTAINER_JVM_MEMORY_XMX_RATIO);

    int workerJvmMemoryOverheadMbs = ConfigUtils.getInt(workerProfileConfig,
        GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY,
        GobblinYarnConfigurationKeys.DEFAULT_CONTAINER_JVM_MEMORY_OVERHEAD_MBS);

    Preconditions.checkArgument(workerJvmMemoryXmxRatio >= 0 && workerJvmMemoryXmxRatio <= 1,
        workerProfileName + " : " + GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY +
            " must be between 0 and 1 inclusive");

    long containerMemoryMbs = container.getResource().getMemorySize();

    Preconditions.checkArgument(workerJvmMemoryOverheadMbs < containerMemoryMbs * workerJvmMemoryXmxRatio,
        workerProfileName + " : " + GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY +
            " cannot be more than " + GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY + " * " +
            GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY);

    Optional<String> workerJvmArgs = workerProfileConfig.hasPath(GobblinYarnConfigurationKeys.CONTAINER_JVM_ARGS_KEY) ?
        Optional.of(workerProfileConfig.getString(GobblinYarnConfigurationKeys.CONTAINER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    // Extract worker class from profile config to pass as system property
    String workerClass = ConfigUtils.getString(workerProfileConfig,
        GobblinTemporalConfigurationKeys.WORKER_CLASS,
        GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS);

    String containerProcessName = GobblinTemporalYarnTaskRunner.class.getSimpleName();
    StringBuilder containerCommand = new StringBuilder()
        .append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
        .append(" -Xmx").append((int) (container.getResource().getMemory() * workerJvmMemoryXmxRatio) -
            workerJvmMemoryOverheadMbs).append("M")
        .append(" -D").append(GobblinYarnConfigurationKeys.JVM_USER_TIMEZONE_CONFIG).append("=").append(this.containerTimezone)
        .append(" -D").append(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_LOG_DIR_NAME).append("=").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR)
        .append(" -D").append(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_LOG_FILE_NAME).append("=").append(containerProcessName).append(".").append(ApplicationConstants.STDOUT)
        .append(" -D").append(GobblinTemporalConfigurationKeys.WORKER_CLASS).append("=").append(workerClass)
        .append(" ").append(JvmUtils.formatJvmArguments(workerJvmArgs))
        .append(" ").append(this.proxyJvmArgs)
        .append(" ").append(GobblinTemporalYarnTaskRunner.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.applicationName)
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME)
        .append(" ").append(this.applicationId);

    return containerCommand.append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
            containerProcessName).append(".").append(ApplicationConstants.STDOUT)
        .append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
            containerProcessName).append(".").append(ApplicationConstants.STDERR).toString();
  }

  /**
   * Handle the completion of a container.
   * Just removes the containerId from {@link #containerMap}
   */
  protected void handleContainerCompletion(ContainerStatus containerStatus) {
    this.containerMap.remove(containerStatus.getContainerId());
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
   * Generates a unique allocation request ID for the given worker profile and store the id to profile mapping.
   *
   * @param workerProfile the worker profile for which the allocation request ID is generated
   * @return the generated allocation request ID
   */
  protected long storeByUniqueAllocationRequestId(WorkerProfile workerProfile) {
    long allocationRequestId = allocationRequestIdGenerator.getAndIncrement();
    this.workerProfileByAllocationRequestId.put(allocationRequestId, workerProfile);
    return allocationRequestId;
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
        long allocationRequestId = container.getAllocationRequestId();
        WorkerProfile workerProfile = Optional.fromNullable(workerProfileByAllocationRequestId.get(allocationRequestId))
            .or(() -> {
              LOGGER.warn("No Worker Profile found for {}, so falling back to default", allocationRequestId);
              return workerProfileByAllocationRequestId.computeIfAbsent(DEFAULT_ALLOCATION_REQUEST_ID, k -> {
                LOGGER.warn("WARNING: (LIKELY) UNEXPECTED CONCURRENCY: No Worker Profile even yet mapped to the default allocation request ID {} - creating one now", DEFAULT_ALLOCATION_REQUEST_ID);
                return new WorkerProfile(config);
              });
            });

        String containerId = container.getId().toString();
        if (eventSubmitter.isPresent()) {
          eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_ALLOCATION,
              GobblinYarnMetricTagNames.CONTAINER_ID, containerId);
        }

        LOGGER.info("Container {} has been allocated with resource {} for Worker Profile {}",
            container.getId(), container.getResource(), WorkforceProfiles.renderName(workerProfile.getName()));

        ContainerInfo containerInfo = new ContainerInfo(container,
            WorkforceProfiles.renderName(workerProfile.getName()), workerProfile);
        containerMap.put(container.getId(), containerInfo);

        // Find matching requests and remove the request (YARN-660). We the scheduler are responsible
        // for cleaning up requests after allocation based on the design in the described ticket.
        // YARN does not have a delta request API and the requests are not cleaned up automatically.
        // Try finding a match first with requestAllocationId (which should always be the case) then fall back to
        // finding a match with the host as the resource name which then will fall back to any resource match.
        // Also see YARN-1902. Container count will explode without this logic for removing container requests.
        Collection<AMRMClient.ContainerRequest> matchingRequestsByAllocationRequestId = amrmClientAsync.getMatchingRequests(container.getAllocationRequestId());
        if (!matchingRequestsByAllocationRequestId.isEmpty()) {
          AMRMClient.ContainerRequest firstMatchingContainerRequest = matchingRequestsByAllocationRequestId.iterator().next();
          LOGGER.info("Found matching requests {}, removing first matching request {}",
              matchingRequestsByAllocationRequestId, firstMatchingContainerRequest);

          amrmClientAsync.removeContainerRequest(firstMatchingContainerRequest);
        } else {
          LOGGER.info("Matching request by allocation request id {} not found", container.getAllocationRequestId());

          List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequestsByHost = amrmClientAsync
              .getMatchingRequests(container.getPriority(), container.getNodeHttpAddress(), container.getResource());

          if (matchingRequestsByHost.isEmpty()) {
            LOGGER.info("Matching request by host {} not found", container.getNodeHttpAddress());

            matchingRequestsByHost = amrmClientAsync
                .getMatchingRequests(container.getPriority(), ResourceRequest.ANY, container.getResource());
          }

          if (!matchingRequestsByHost.isEmpty()) {
            AMRMClient.ContainerRequest firstMatchingContainerRequest = matchingRequestsByHost.get(0).iterator().next();
            LOGGER.info("Found matching requests {}, removing first matching request {}",
                matchingRequestsByAllocationRequestId, firstMatchingContainerRequest);

            amrmClientAsync.removeContainerRequest(firstMatchingContainerRequest);
          }
        }

        containerLaunchExecutor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              LOGGER.info("Starting container " + containerId);
              nmClientAsync.startContainerAsync(container, newContainerLaunchContext(containerInfo));
            } catch (IOException ioe) {
              LOGGER.error("Failed to start container " + containerId, ioe);
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
  class NMClientCallbackHandler implements NMClientAsync.CallbackHandler {

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

  // Class encapsulates Container instance, WorkerProfile name to print, WorkerProfile, and
  // initial startup command
  @Getter
  class ContainerInfo {
    private final Container container;
    private final String workerProfileName; // Storing this to avoid calling WorkforceProfiles.renderName(workerProfile.getName()) while logging
    private final WorkerProfile workerProfile;
    private final String startupCommand;

    public ContainerInfo(Container container, String workerProfileName, WorkerProfile workerProfile) {
      this.container = container;
      this.workerProfileName = workerProfileName;
      this.workerProfile = workerProfile;
      this.startupCommand = YarnService.this.buildContainerCommand(container, workerProfileName, workerProfile);
    }

    @Override
    public String toString() {
      return String.format("ContainerInfo{ container=%s, workerProfileName=%s, startupCommand=%s }",
          container.getId(), workerProfileName, startupCommand);
    }
  }
}

