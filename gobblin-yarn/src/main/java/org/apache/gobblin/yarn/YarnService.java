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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import lombok.AllArgsConstructor;
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
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import lombok.AccessLevel;
import lombok.Getter;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.cluster.GobblinClusterMetricTagNames;
import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.cluster.HelixUtils;
import org.apache.gobblin.cluster.event.ClusterManagerShutdownRequest;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricReporterException;
import org.apache.gobblin.metrics.MultiReporterException;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.ExecutorsUtils;
import org.apache.gobblin.util.JvmUtils;
import org.apache.gobblin.util.executors.ScalingThreadPoolExecutor;
import org.apache.gobblin.yarn.event.ContainerReleaseRequest;
import org.apache.gobblin.yarn.event.ContainerShutdownRequest;
import org.apache.gobblin.yarn.event.NewContainerRequest;

import static org.apache.gobblin.yarn.GobblinYarnTaskRunner.HELIX_YARN_INSTANCE_NAME_PREFIX;

/**
 * This class is responsible for all Yarn-related stuffs including ApplicationMaster registration,
 * ApplicationMaster un-registration, Yarn container management, etc.
 *
 * @author Yinan Li
 */
public class YarnService extends AbstractIdleService {

  private static final Logger LOGGER = LoggerFactory.getLogger(YarnService.class);

  private static final String UNKNOWN_HELIX_INSTANCE = "UNKNOWN";

  private final String applicationName;
  private final String applicationId;
  private final String appViewAcl;
  //Default helix instance tag derived from cluster level config
  private final String helixInstanceTags;

  private final Config config;

  private final EventBus eventBus;

  private final Configuration yarnConfiguration;
  private final FileSystem fs;

  private final Optional<GobblinMetrics> gobblinMetrics;
  private final Optional<EventSubmitter> eventSubmitter;

  @VisibleForTesting
  @Getter(AccessLevel.PROTECTED)
  private final AMRMClientAsync<AMRMClient.ContainerRequest> amrmClientAsync;
  private final NMClientAsync nmClientAsync;
  private final ExecutorService containerLaunchExecutor;

  private final int initialContainers;
  private final int requestedContainerMemoryMbs;
  private final int requestedContainerCores;
  private final int jvmMemoryOverheadMbs;
  private final double jvmMemoryXmxRatio;
  private final boolean containerHostAffinityEnabled;

  private final int helixInstanceMaxRetries;

  private final Optional<String> containerJvmArgs;
  private final String containerTimezone;
  private final HelixManager helixManager;
  private final HelixAdmin helixAdmin;

  @Getter(AccessLevel.PROTECTED)
  private volatile Optional<Resource> maxResourceCapacity = Optional.absent();

  // Security tokens for accessing HDFS
  private ByteBuffer tokens;

  private final Closer closer = Closer.create();

  private final Object allContainersStopped = new Object();

  // A map from container IDs to Container instances, Helix participant IDs of the containers and Helix Tag
  @VisibleForTesting
  @Getter(AccessLevel.PROTECTED)
  private final ConcurrentMap<ContainerId, ContainerInfo> containerMap = Maps.newConcurrentMap();

  // A cache of the containers with an outstanding container release request.
  // This is a cache instead of a set to get the automatic cleanup in case a container completes before the requested
  // release.
  @VisibleForTesting
  @Getter(AccessLevel.PROTECTED)
  private final Cache<ContainerId, String> releasedContainerCache;

  // A generator for an integer ID of a Helix instance (participant)
  private final AtomicInteger helixInstanceIdGenerator = new AtomicInteger(0);

  // A map from Helix instance names to the number times the instances are retried to be started
  private final ConcurrentMap<String, AtomicInteger> helixInstanceRetryCount = Maps.newConcurrentMap();

  // A concurrent HashSet of unused Helix instance names. An unused Helix instance name gets put
  // into the set if the container running the instance completes. Unused Helix
  // instance names get picked up when replacement containers get allocated.
  private final Set<String> unusedHelixInstanceNames = ConcurrentHashMap.newKeySet();

  // The map from helix tag to allocated container count
  private final ConcurrentMap<String, AtomicInteger> allocatedContainerCountMap = Maps.newConcurrentMap();

  private final boolean isPurgingOfflineHelixInstancesEnabled;
  private final long helixPurgeLaggingThresholdMs;
  private final long helixPurgeStatusPollingRateMs;

  private volatile YarnContainerRequestBundle yarnContainerRequest;
  private final AtomicInteger priorityNumGenerator = new AtomicInteger(0);
  private final Map<String, Integer> resourcePriorityMap = new HashMap<>();

  private volatile boolean shutdownInProgress = false;

  public YarnService(Config config, String applicationName, String applicationId, YarnConfiguration yarnConfiguration,
      FileSystem fs, EventBus eventBus, HelixManager helixManager, HelixAdmin helixAdmin) throws Exception {
    this.applicationName = applicationName;
    this.applicationId = applicationId;

    this.config = config;

    this.eventBus = eventBus;

    this.helixManager = helixManager;
    this.helixAdmin = helixAdmin;

    this.gobblinMetrics = config.getBoolean(ConfigurationKeys.METRICS_ENABLED_KEY) ?
        Optional.of(buildGobblinMetrics()) : Optional.<GobblinMetrics>absent();

    this.eventSubmitter = config.getBoolean(ConfigurationKeys.METRICS_ENABLED_KEY) ?
        Optional.of(buildEventSubmitter()) : Optional.<EventSubmitter>absent();

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

    this.initialContainers = config.getInt(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY);
    this.requestedContainerMemoryMbs = config.getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    this.requestedContainerCores = config.getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY);
    this.containerHostAffinityEnabled = config.getBoolean(GobblinYarnConfigurationKeys.CONTAINER_HOST_AFFINITY_ENABLED);

    this.helixInstanceMaxRetries = config.getInt(GobblinYarnConfigurationKeys.HELIX_INSTANCE_MAX_RETRIES);
    this.helixInstanceTags = ConfigUtils.getString(config,
        GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY, GobblinClusterConfigurationKeys.HELIX_DEFAULT_TAG);
    this.isPurgingOfflineHelixInstancesEnabled = ConfigUtils.getBoolean(config,
        GobblinYarnConfigurationKeys.HELIX_PURGE_OFFLINE_INSTANCES_ENABLED,
        GobblinYarnConfigurationKeys.DEFAULT_HELIX_PURGE_OFFLINE_INSTANCES_ENABLED);
    this.helixPurgeLaggingThresholdMs = ConfigUtils.getLong(config,
        GobblinYarnConfigurationKeys.HELIX_PURGE_LAGGING_THRESHOLD_MILLIS,
        GobblinYarnConfigurationKeys.DEFAULT_HELIX_PURGE_LAGGING_THRESHOLD_MILLIS);
    this.helixPurgeStatusPollingRateMs = ConfigUtils.getLong(config,
        GobblinYarnConfigurationKeys.HELIX_PURGE_POLLING_RATE_MILLIS,
        GobblinYarnConfigurationKeys.DEFAULT_HELIX_PURGE_POLLING_RATE_MILLIS);

    this.containerJvmArgs = config.hasPath(GobblinYarnConfigurationKeys.CONTAINER_JVM_ARGS_KEY) ?
        Optional.of(config.getString(GobblinYarnConfigurationKeys.CONTAINER_JVM_ARGS_KEY)) :
        Optional.<String>absent();

    int numContainerLaunchThreads =
        ConfigUtils.getInt(config, GobblinYarnConfigurationKeys.MAX_CONTAINER_LAUNCH_THREADS_KEY,
            GobblinYarnConfigurationKeys.DEFAULT_MAX_CONTAINER_LAUNCH_THREADS);
    this.containerLaunchExecutor = ScalingThreadPoolExecutor.newScalingThreadPool(5, numContainerLaunchThreads, 0L,
        ExecutorsUtils.newThreadFactory(Optional.of(LOGGER), Optional.of("ContainerLaunchExecutor")));

    this.tokens = getSecurityTokens();

    this.releasedContainerCache = CacheBuilder.newBuilder().expireAfterAccess(ConfigUtils.getInt(config,
        GobblinYarnConfigurationKeys.RELEASED_CONTAINERS_CACHE_EXPIRY_SECS,
        GobblinYarnConfigurationKeys.DEFAULT_RELEASED_CONTAINERS_CACHE_EXPIRY_SECS), TimeUnit.SECONDS).build();

    this.jvmMemoryXmxRatio = ConfigUtils.getDouble(this.config,
        GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY,
        GobblinYarnConfigurationKeys.DEFAULT_CONTAINER_JVM_MEMORY_XMX_RATIO);

    Preconditions.checkArgument(this.jvmMemoryXmxRatio >= 0 && this.jvmMemoryXmxRatio <= 1,
        GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY + " must be between 0 and 1 inclusive");

    this.jvmMemoryOverheadMbs = ConfigUtils.getInt(this.config,
        GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY,
        GobblinYarnConfigurationKeys.DEFAULT_CONTAINER_JVM_MEMORY_OVERHEAD_MBS);

    Preconditions.checkArgument(this.jvmMemoryOverheadMbs < this.requestedContainerMemoryMbs * this.jvmMemoryXmxRatio,
        GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY + " cannot be more than "
            + GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY + " * "
            + GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY);

    this.appViewAcl = ConfigUtils.getString(this.config, GobblinYarnConfigurationKeys.APP_VIEW_ACL,
        GobblinYarnConfigurationKeys.DEFAULT_APP_VIEW_ACL);
    this.containerTimezone = ConfigUtils.getString(this.config, GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_TIMEZONE,
        GobblinYarnConfigurationKeys.DEFAULT_GOBBLIN_YARN_CONTAINER_TIMEZONE);
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
    requestContainer(newContainerRequest.getReplacedContainer().transform(container -> container.getNodeId().getHost()),
        newContainerRequest.getResource());
  }

  protected NMClientCallbackHandler getNMClientCallbackHandler() {
    return new NMClientCallbackHandler();
  }

  @SuppressWarnings("unused")
  @Subscribe
  public void handleContainerShutdownRequest(ContainerShutdownRequest containerShutdownRequest) {
    for (Container container : containerShutdownRequest.getContainers()) {
      LOGGER.info(String.format("Stopping container %s running on %s", container.getId(), container.getNodeId()));
      this.nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
    }
  }

  /**
   * Request the Resource Manager to release the container
   * @param containerReleaseRequest containers to release
   */
  @Subscribe
  public void handleContainerReleaseRequest(ContainerReleaseRequest containerReleaseRequest) {
    for (Container container : containerReleaseRequest.getContainers()) {
      LOGGER.info(String.format("Releasing container %s running on %s", container.getId(), container.getNodeId()));

      // Record that this container was explicitly released so that a new one is not spawned to replace it
      // Put the container id in the releasedContainerCache before releasing it so that handleContainerCompletion()
      // can check for the container id and skip spawning a replacement container.
      // Note that this is best effort since these are asynchronous operations and a container may abort concurrently
      // with the release call. So in some cases a replacement container may have already been spawned before
      // the container is put into the black list.
      this.releasedContainerCache.put(container.getId(), "");
      this.amrmClientAsync.releaseAssignedContainer(container.getId());
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

    if (this.isPurgingOfflineHelixInstancesEnabled) {
      purgeHelixOfflineInstances(this.helixPurgeLaggingThresholdMs);
    }

    LOGGER.info("Requesting initial containers");
    requestInitialContainers(this.initialContainers);
  }

  private void purgeHelixOfflineInstances(long laggingThresholdMs) {
    LOGGER.info("Purging offline helix instances before allocating containers for helixClusterName={}, connectionString={}, helixPurgeStatusPollingRateMs={}",
        helixManager.getClusterName(), helixManager.getMetadataStoreConnectionString(), this.helixPurgeStatusPollingRateMs);
    HelixInstancePurgerWithMetrics purger = new HelixInstancePurgerWithMetrics(this.eventSubmitter.orNull(),
        this.helixPurgeStatusPollingRateMs);
    Map<String, String> gteMetadata = ImmutableMap.of(
        "connectionString", this.helixManager.getMetadataStoreConnectionString(),
        "clusterName", this.helixManager.getClusterName()
    );
    purger.purgeAllOfflineInstances(this.helixAdmin, this.helixManager.getClusterName(), laggingThresholdMs, gteMetadata);
  }

  @Override
  protected void shutDown() throws IOException {
    LOGGER.info("Stopping the YarnService");

    this.shutdownInProgress = true;

    try {
      ExecutorsUtils.shutdownExecutorService(this.containerLaunchExecutor, Optional.of(LOGGER));

      // Stop the running containers
      for (ContainerInfo containerInfo : this.containerMap.values()) {
        LOGGER.info("Stopping container {} running participant {}", containerInfo.getContainer().getId(),
            containerInfo.getHelixParticipantId());
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

  /**
   * Request an allocation of containers. If numTargetContainers is larger than the max of current and expected number
   * of containers then additional containers are requested.
   *
   * If numTargetContainers is less than the current number of allocated containers then release free containers.
   * Shrinking is relative to the number of currently allocated containers since it takes time for containers
   * to be allocated and assigned work and we want to avoid releasing a container prematurely before it is assigned
   * work. This means that a container may not be released even though numTargetContainers is less than the requested
   * number of containers. The intended usage is for the caller of this method to make periodic calls to attempt to
   * adjust the cluster towards the desired number of containers.
   *
   * @param yarnContainerRequestBundle the desired containers information, including numbers, resource and helix tag
   * @param inUseInstances  a set of in use instances
   */
  public synchronized void requestTargetNumberOfContainers(YarnContainerRequestBundle yarnContainerRequestBundle, Set<String> inUseInstances) {
    LOGGER.info("Trying to set numTargetContainers={}, in-use helix instances count is {}, container map size is {}",
        yarnContainerRequestBundle.getTotalContainers(), inUseInstances.size(), this.containerMap.size());
    int numTargetContainers = yarnContainerRequestBundle.getTotalContainers();
    // YARN can allocate more than the requested number of containers, compute additional allocations and deallocations
    // based on the max of the requested and actual allocated counts
    // Represents the number of containers allocated for across all helix tags
    int totalAllocatedContainers = this.containerMap.size();

    // Request additional containers if the desired count is higher than the max of the current allocation or previously
    // requested amount. Note that there may be in-flight or additional allocations after numContainers has been computed
    // so overshooting can occur, but periodic calls to this method will make adjustments towards the target.
    for (Map.Entry<String, Integer> entry : yarnContainerRequestBundle.getHelixTagContainerCountMap().entrySet()) {
      String currentHelixTag = entry.getKey();
      int desiredContainerCount = entry.getValue();
      Resource resourceForHelixTag = yarnContainerRequestBundle.getHelixTagResourceMap().get(currentHelixTag);

      // Calculate requested container count based on adding allocated count and outstanding ContainerRequests in Yarn
      allocatedContainerCountMap.putIfAbsent(currentHelixTag, new AtomicInteger(0));
      int allocatedContainersForHelixTag = allocatedContainerCountMap.get(currentHelixTag).get();
      int outstandingContainerRequests = getMatchingRequestsCount(resourceForHelixTag);
      int requestedContainerCount = allocatedContainersForHelixTag + outstandingContainerRequests;
      int numContainersNeeded = desiredContainerCount - requestedContainerCount;
      LOGGER.info("Container counts for helixTag={} (allocatedContainers={}, outstandingContainerRequests={}, desiredContainerCount={}, numContainersNeeded={})",
          currentHelixTag, allocatedContainersForHelixTag, outstandingContainerRequests, desiredContainerCount, numContainersNeeded);

      if (numContainersNeeded > 0) {
        requestContainers(numContainersNeeded, resourceForHelixTag);
      }
    }

    // If the total desired is lower than the currently allocated amount then release free containers.
    // This is based on the currently allocated amount since containers may still be in the process of being allocated
    // and assigned work. Resizing based on numRequestedContainers at this point may release a container right before
    // or soon after it is assigned work.
    if (numTargetContainers < totalAllocatedContainers) {
      List<Container> containersToRelease = new ArrayList<>();
      int numToShutdown = totalAllocatedContainers - numTargetContainers;

      LOGGER.info("Shrinking number of containers by {} because numTargetContainers < totalAllocatedContainers ({} < {})",
          numToShutdown, numTargetContainers, totalAllocatedContainers);

      // Look for eligible containers to release. If a container is in use then it is not released.
      for (Map.Entry<ContainerId, ContainerInfo> entry : this.containerMap.entrySet()) {
        ContainerInfo containerInfo = entry.getValue();
        if (!inUseInstances.contains(containerInfo.getHelixParticipantId())) {
          containersToRelease.add(containerInfo.getContainer());
        }

        if (containersToRelease.size() == numToShutdown) {
          break;
        }
      }

      LOGGER.info("Shutting down {} containers. containersToRelease={}", containersToRelease.size(), containersToRelease);

      this.eventBus.post(new ContainerReleaseRequest(containersToRelease));
    }
    this.yarnContainerRequest = yarnContainerRequestBundle;
    LOGGER.info("Current tag-container desired count:{}, tag-container allocated: {}",
        yarnContainerRequestBundle.getHelixTagContainerCountMap(), this.allocatedContainerCountMap);
  }

  // Request initial containers with default resource and helix tag
  private void requestInitialContainers(int containersRequested) {
    YarnContainerRequestBundle initialYarnContainerRequest = new YarnContainerRequestBundle();
    Resource capability = Resource.newInstance(this.requestedContainerMemoryMbs, this.requestedContainerCores);
    initialYarnContainerRequest.add(this.helixInstanceTags, containersRequested, capability);
    requestTargetNumberOfContainers(initialYarnContainerRequest, Collections.EMPTY_SET);
  }

  private void requestContainer(Optional<String> preferredNode, Optional<Resource> resourceOptional) {
    Resource desiredResource = resourceOptional.or(Resource.newInstance(
        this.requestedContainerMemoryMbs, this.requestedContainerCores));
    requestContainer(preferredNode, desiredResource);
  }

  /**
   * Request {@param numContainers} from yarn with the specified resource. Resources will be allocated without a preferred
   * node
   * @param numContainers
   * @param resource
   */
  private void requestContainers(int numContainers, Resource resource) {
    LOGGER.info("Requesting {} containers with resource={}", numContainers, resource);
    IntStream.range(0, numContainers)
        .forEach(i -> requestContainer(Optional.absent(), resource));
  }

  // Request containers with specific resource requirement
  private void requestContainer(Optional<String> preferredNode, Resource resource) {
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
    this.amrmClientAsync.addContainerRequest(
        new AMRMClient.ContainerRequest(resource, preferredNodes, null, priority));
  }

  protected ContainerLaunchContext newContainerLaunchContext(ContainerInfo containerInfo)
      throws IOException {
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPathFromConfig(this.config, this.fs, this.applicationName, this.applicationId);
    Path containerWorkDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.CONTAINER_WORK_DIR_NAME);

    Map<String, LocalResource> resourceMap = Maps.newHashMap();

    addContainerLocalResources(new Path(appWorkDir, GobblinYarnConfigurationKeys.LIB_JARS_DIR_NAME), resourceMap);
    addContainerLocalResources(new Path(containerWorkDir, GobblinYarnConfigurationKeys.APP_JARS_DIR_NAME), resourceMap);
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
    containerLaunchContext.setCommands(Lists.newArrayList(buildContainerCommand(containerInfo)));

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
      for (FileStatus status : statuses) {
        YarnHelixUtils.addFileAsLocalResource(this.fs, status.getPath(), LocalResourceType.FILE, resourceMap);
      }
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

  @VisibleForTesting
  protected String buildContainerCommand(ContainerInfo containerInfo) {
    String containerProcessName = GobblinYarnTaskRunner.class.getSimpleName();
    StringBuilder containerCommand = new StringBuilder()
        .append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
        .append(" -Xmx").append((int) (containerInfo.getContainer().getResource().getMemory() * this.jvmMemoryXmxRatio) -
            this.jvmMemoryOverheadMbs).append("M")
        .append(" -D").append(GobblinYarnConfigurationKeys.JVM_USER_TIMEZONE_CONFIG).append("=").append(this.containerTimezone)
        .append(" -D").append(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_LOG_DIR_NAME).append("=").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR)
        .append(" -D").append(GobblinYarnConfigurationKeys.GOBBLIN_YARN_CONTAINER_LOG_FILE_NAME).append("=").append(containerProcessName).append(".").append(ApplicationConstants.STDOUT)
        .append(" ").append(JvmUtils.formatJvmArguments(this.containerJvmArgs))
        .append(" ").append(GobblinYarnTaskRunner.class.getName())
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_NAME_OPTION_NAME)
        .append(" ").append(this.applicationName)
        .append(" --").append(GobblinClusterConfigurationKeys.APPLICATION_ID_OPTION_NAME)
        .append(" ").append(this.applicationId)
        .append(" --").append(GobblinClusterConfigurationKeys.HELIX_INSTANCE_NAME_OPTION_NAME)
        .append(" ").append(containerInfo.getHelixParticipantId());

    if (!Strings.isNullOrEmpty(containerInfo.getHelixTag())) {
      containerCommand.append(" --").append(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_OPTION_NAME)
          .append(" ").append(containerInfo.getHelixTag());
    }
    return containerCommand.append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
        containerProcessName).append(".").append(ApplicationConstants.STDOUT)
        .append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append(File.separator).append(
            containerProcessName).append(".").append(ApplicationConstants.STDERR).toString();
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
  protected void handleContainerCompletion(ContainerStatus containerStatus) {
    ContainerInfo completedContainerInfo = this.containerMap.remove(containerStatus.getContainerId());
    //Get the Helix instance name for the completed container. Because callbacks are processed asynchronously, we might
    //encounter situations where handleContainerCompletion() is called before onContainersAllocated(), resulting in the
    //containerId missing from the containersMap.
    String completedInstanceName = completedContainerInfo == null?  UNKNOWN_HELIX_INSTANCE : completedContainerInfo.getHelixParticipantId();
    String helixTag = completedContainerInfo == null ? helixInstanceTags : completedContainerInfo.getHelixTag();
    allocatedContainerCountMap.get(helixTag).decrementAndGet();

    LOGGER.info(String.format("Container %s running Helix instance %s with tag %s has completed with exit status %d",
        containerStatus.getContainerId(), completedInstanceName, helixTag, containerStatus.getExitStatus()));

    if (!Strings.isNullOrEmpty(containerStatus.getDiagnostics())) {
      LOGGER.info(String.format("Received the following diagnostics information for container %s: %s",
          containerStatus.getContainerId(), containerStatus.getDiagnostics()));
    }

    if (containerStatus.getExitStatus() == ContainerExitStatus.ABORTED) {
      if (this.releasedContainerCache.getIfPresent(containerStatus.getContainerId()) != null) {
        LOGGER.info("Container release requested, so not spawning a replacement for containerId {}", containerStatus.getContainerId());
        if (completedContainerInfo != null) {
          LOGGER.info("Adding instance {} to the pool of unused instances", completedInstanceName);
          this.unusedHelixInstanceNames.add(completedInstanceName);
        }
        return;
      } else {
        LOGGER.info("Container {} aborted due to lost NM", containerStatus.getContainerId());
        // Container release was not requested. Likely, the container was running on a node on which the NM died.
        // In this case, RM assumes that the containers are "lost", even though the container process may still be
        // running on the node. We need to ensure that the Helix instances running on the orphaned containers
        // are fenced off from the Helix cluster to avoid double publishing and state being committed by the
        // instances.
        if (!UNKNOWN_HELIX_INSTANCE.equals(completedInstanceName)) {
          String clusterName = this.helixManager.getClusterName();
          //Disable the orphaned instance.
          if (HelixUtils.isInstanceLive(helixManager, completedInstanceName)) {
            LOGGER.info("Disabling the Helix instance {}", completedInstanceName);
            this.helixManager.getClusterManagmentTool().enableInstance(clusterName, completedInstanceName, false);
          }
        }
      }
    }

    if (this.shutdownInProgress) {
      return;
    }
    if(completedContainerInfo != null) {
      this.helixInstanceRetryCount.putIfAbsent(completedInstanceName, new AtomicInteger(0));
      int retryCount = this.helixInstanceRetryCount.get(completedInstanceName).incrementAndGet();

      // Populate event metadata
      Optional<ImmutableMap.Builder<String, String>> eventMetadataBuilder = Optional.absent();
      if (this.eventSubmitter.isPresent()) {
        eventMetadataBuilder = Optional.of(buildContainerStatusEventMetadata(containerStatus));
        eventMetadataBuilder.get().put(GobblinYarnEventConstants.EventMetadata.HELIX_INSTANCE_ID, completedInstanceName);
        eventMetadataBuilder.get().put(GobblinYarnEventConstants.EventMetadata.CONTAINER_STATUS_RETRY_ATTEMPT, retryCount + "");
      }

      if (this.helixInstanceMaxRetries > 0 && retryCount > this.helixInstanceMaxRetries) {
        if (this.eventSubmitter.isPresent()) {
          this.eventSubmitter.get()
              .submit(GobblinYarnEventConstants.EventNames.HELIX_INSTANCE_COMPLETION, eventMetadataBuilder.get().build());
        }

        LOGGER.warn("Maximum number of retries has been achieved for Helix instance " + completedInstanceName);
        return;
      }

      // Add the Helix instance name of the completed container to the set of unused
      // instance names so they can be reused by a replacement container.
      LOGGER.info("Adding instance {} to the pool of unused instances", completedInstanceName);
      this.unusedHelixInstanceNames.add(completedInstanceName);

      if (this.eventSubmitter.isPresent()) {
        this.eventSubmitter.get()
            .submit(GobblinYarnEventConstants.EventNames.HELIX_INSTANCE_COMPLETION, eventMetadataBuilder.get().build());
      }
    }
    Optional<Resource> newContainerResource = completedContainerInfo != null ?
        Optional.of(completedContainerInfo.getContainer().getResource()) : Optional.absent();
    LOGGER.info("Requesting a new container to replace {} to run Helix instance {} with helix tag {} and resource {}",
        containerStatus.getContainerId(), completedInstanceName, helixTag, newContainerResource.orNull());
    this.eventBus.post(new NewContainerRequest(
        shouldStickToTheSameNode(containerStatus.getExitStatus()) && completedContainerInfo != null ?
            Optional.of(completedContainerInfo.getContainer()) : Optional.absent(), newContainerResource));
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
   * Get the number of matching container requests for the specified resource memory and cores.
   * Due to YARN-1902 and YARN-660, this API is not 100% accurate. {@link AMRMClientCallbackHandler#onContainersAllocated(List)}
   * contains logic for best effort clean up of requests, and the resource tend to match the allocated container. So in practice the count is pretty accurate.
   *
   * This API call gets the count of container requests for containers that are > resource if there is no request with the exact same resource
   * The RM can return containers that are larger (because of normalization etc).
   * Container may be larger by memory or cpu (e.g. container (1000M, 3cpu) can fit request (1000M, 1cpu) or request (500M, 3cpu).
   *
   * Thankfully since each helix tag / resource has a different priority, matching requests for one helix tag / resource
   * have complete isolation from another helix tag / resource
   */
  private int getMatchingRequestsCount(Resource resource) {
    Integer priorityNum = resourcePriorityMap.get(resource.toString());
    if (priorityNum == null) { // request has never been made with this resource
      return 0;
    }
    Priority priority = Priority.newInstance(priorityNum);

    // Each collection in the list represents a set of requests with each with the same resource requirement.
    // The reason for differing resources can be due to normalization
    List<? extends Collection<AMRMClient.ContainerRequest>> outstandingRequests = getAmrmClientAsync().getMatchingRequests(priority, ResourceRequest.ANY, resource);
    return outstandingRequests == null ? 0 : outstandingRequests.stream()
        .filter(Objects::nonNull)
        .mapToInt(Collection::size)
        .sum();
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
        String containerId = container.getId().toString();
        String containerHelixTag = YarnHelixUtils.findHelixTagForContainer(container, allocatedContainerCountMap, yarnContainerRequest);
        if (Strings.isNullOrEmpty(containerHelixTag)) {
          containerHelixTag = helixInstanceTags;
        }
        if (eventSubmitter.isPresent()) {
          eventSubmitter.get().submit(GobblinYarnEventConstants.EventNames.CONTAINER_ALLOCATION,
              GobblinYarnMetricTagNames.CONTAINER_ID, containerId);
        }

        LOGGER.info("Container {} has been allocated with resource {} for helix tag {}",
            container.getId(), container.getResource(), containerHelixTag);

        //Iterate over the (thread-safe) set of unused instances to find the first instance that is not currently live.
        //Once we find a candidate instance, it is removed from the set.
        String instanceName = null;

        //Ensure that updates to unusedHelixInstanceNames are visible to other threads that might concurrently
        //invoke the callback on container allocation.
        synchronized (this) {
          Iterator<String> iterator = unusedHelixInstanceNames.iterator();
          while (iterator.hasNext()) {
            instanceName = iterator.next();
            if (!HelixUtils.isInstanceLive(helixManager, instanceName)) {
              iterator.remove();
              LOGGER.info("Found an unused instance {}", instanceName);
              break;
            } else {
              //Reset the instance name to null, since this instance name is live.
              instanceName = null;
            }
          }
        }

        if (Strings.isNullOrEmpty(instanceName)) {
          // No unused instance name, so generating a new one.
          instanceName = HelixUtils
              .getHelixInstanceName(HELIX_YARN_INSTANCE_NAME_PREFIX, helixInstanceIdGenerator.incrementAndGet());
        }

        ContainerInfo containerInfo = new ContainerInfo(container, instanceName, containerHelixTag);
        containerMap.put(container.getId(), containerInfo);
        allocatedContainerCountMap.putIfAbsent(containerHelixTag, new AtomicInteger(0));
        allocatedContainerCountMap.get(containerHelixTag).incrementAndGet();

        // Find matching requests and remove the request (YARN-660). We the scheduler are responsible
        // for cleaning up requests after allocation based on the design in the described ticket.
        // YARN does not have a delta request API and the requests are not cleaned up automatically.
        // Try finding a match first with the host as the resource name then fall back to any resource match.
        // Also see YARN-1902. Container count will explode without this logic for removing container requests.
        List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequests = amrmClientAsync
            .getMatchingRequests(container.getPriority(), container.getNodeHttpAddress(), container.getResource());

        if (matchingRequests.isEmpty()) {
          LOGGER.debug("Matching request by host {} not found", container.getNodeHttpAddress());

          matchingRequests = amrmClientAsync
              .getMatchingRequests(container.getPriority(), ResourceRequest.ANY, container.getResource());
        }

        if (!matchingRequests.isEmpty()) {
          AMRMClient.ContainerRequest firstMatchingContainerRequest = matchingRequests.get(0).iterator().next();
          LOGGER.debug("Found matching requests {}, removing first matching request {}",
              matchingRequests, firstMatchingContainerRequest);

          amrmClientAsync.removeContainerRequest(firstMatchingContainerRequest);
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

  //A class encapsulates Container instances, Helix participant IDs of the containers and Helix Tag
  @AllArgsConstructor
  @Getter
  static class ContainerInfo {
    private final Container container;
    private final String helixParticipantId;
    private final String helixTag;
  }
}
