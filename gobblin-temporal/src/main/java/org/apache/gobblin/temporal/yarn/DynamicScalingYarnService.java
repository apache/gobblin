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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.workflow.WorkflowStage;
import org.apache.gobblin.temporal.dynamic.ProfileDerivation;
import org.apache.gobblin.temporal.dynamic.ProfileOverlay;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.StaffingDeltas;
import org.apache.gobblin.temporal.dynamic.WorkerProfile;
import org.apache.gobblin.temporal.dynamic.WorkforcePlan;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;
import org.apache.gobblin.temporal.dynamic.WorkforceStaffing;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;

/**
 * Service for dynamically scaling Gobblin containers running on YARN.
 * This service manages workforce staffing and plans, and requests new containers as needed.
 */
@Slf4j
public class DynamicScalingYarnService extends YarnService {
  private static final String DEFAULT_REPLACEMENT_CONTAINER_WORKER_PROFILE_NAME_PREFIX = "replacementWorkerProfile";
  private static final int LAUNCH_CONTAINER_FAILED_EXIT_CODE = 1;
  protected static final int GENERAL_OOM_EXIT_STATUS_CODE = 137;
  protected static final int DEFAULT_REPLACEMENT_CONTAINER_MEMORY_MULTIPLIER = 2;
  private static final int MAX_REPLACEMENT_CONTAINER_MEMORY_MBS = 65536; // 64GB
  private static final int EPSILON_MIILIS = 1;

  /** this holds the current count of containers already requested for each worker profile */
  private final WorkforceStaffing actualWorkforceStaffing;
  /** this holds the current total workforce plan as per latest received scaling directives */
  private final WorkforcePlan workforcePlan;
  private final int initialContainers;
  protected final Queue<ContainerId> removedContainerIds;
  private final AtomicLong profileNameSuffixGenerator;
  private final boolean dynamicScalingEnabled;

  public DynamicScalingYarnService(Config config, String applicationName, String applicationId,
      YarnConfiguration yarnConfiguration, FileSystem fs, EventBus eventBus) throws Exception {
    super(config, applicationName, applicationId, yarnConfiguration, fs, eventBus);

    this.dynamicScalingEnabled = ConfigUtils.getBoolean(config,
        GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, false);
    this.initialContainers = this.config.getInt(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY);

    this.actualWorkforceStaffing = WorkforceStaffing.initialize(0);
    // Initialize workforce plan:
    // - For dynamic scaling: start with 0 baseline, then add stage-specific profiles
    // - For traditional mode: initialize baseline with configured initial containers
    int baselineSetPoint = this.dynamicScalingEnabled ? 0 : this.initialContainers;
    this.workforcePlan = new WorkforcePlan(this.config, baselineSetPoint);
    this.removedContainerIds = new ConcurrentLinkedQueue<>();
    this.profileNameSuffixGenerator = new AtomicLong();

    // For dynamic scaling, add stage-specific profiles derived from baseline
    if (this.dynamicScalingEnabled) {
      initializeDynamicScalingProfiles();
    }
  }

  /**
   * Initializes stage-specific worker profiles for dynamic scaling mode.
   * Creates two profiles derived from baseline:
   * 1. DiscoveryCommitWorker - for discovery/commit activities
   * 2. ExecutionWorker - for execution activities
   */
  private void initializeDynamicScalingProfiles() {
    log.info("Initializing stage-specific profiles");
    long currTimeMillis = System.currentTimeMillis();
    List<ScalingDirective> initialDirectives = new ArrayList<>();

    // Container 1: DiscoveryCommitWorker (for discovery/commit activities)
    ProfileOverlay discoveryCommitOverlay = createDiscoveryCommitProfileOverlay();
    initialDirectives.add(new ScalingDirective(
        "initial-discovery-commit",
        1, // setPoint = 1 container
        currTimeMillis,
        WorkforceProfiles.BASELINE_NAME,
        discoveryCommitOverlay
    ));

    // Container 2: ExecutionWorker (for execution activities)
    ProfileOverlay executionOverlay = createExecutionProfileOverlay();
    initialDirectives.add(new ScalingDirective(
        "initial-execution",
        1, // setPoint = 1 container
        currTimeMillis + EPSILON_MIILIS,
        WorkforceProfiles.BASELINE_NAME,
        executionOverlay
    ));

    // Apply initial directives to workforce plan
    this.workforcePlan.reviseWhenNewer(initialDirectives, ire -> {
      log.error("Failed to create stage-specific profiles", ire);
      throw new RuntimeException("Failed to initialize stage-specific profiles for dynamic scaling", ire);
    });

    log.info("Initialized {} stage-specific profiles for dynamic scaling", initialDirectives.size());
  }

  @Override
  protected synchronized void requestInitialContainers() {
    log.info("Requesting initial containers based on workforce plan");
    // Calculate deltas between plan and current staffing, then request containers
    StaffingDeltas deltas = this.workforcePlan.calcStaffingDeltas(this.actualWorkforceStaffing);
    requestNewContainersForStaffingDeltas(deltas);
  }

  private ProfileOverlay createDiscoveryCommitProfileOverlay() {
    List<ProfileOverlay.KVPair> overlayPairs = new ArrayList<>();

    // Set worker class
    overlayPairs.add(new ProfileOverlay.KVPair(
        GobblinTemporalConfigurationKeys.WORKER_CLASS,
        "org.apache.gobblin.temporal.ddm.worker.DiscoveryCommitWorker"
    ));

    // Set Helix tag
    overlayPairs.add(new ProfileOverlay.KVPair(
        GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY,
        "discovery-commit"
    ));

    // Set stage-specific memory (discovery/commit operations are typically lightweight)
    // Falls back to global CONTAINER_MEMORY_MBS_KEY if stage-specific memory not configured
    if (this.config.hasPath(GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_MEMORY_MB)) {
      String discoveryCommitMemoryMb = this.config.getString(
          GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_MEMORY_MB);
      overlayPairs.add(new ProfileOverlay.KVPair(
          GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
          discoveryCommitMemoryMb
      ));
      log.info("Setting discovery-commit container memory to {} MB", discoveryCommitMemoryMb);
    }

    return new ProfileOverlay.Adding(overlayPairs);
  }

  private ProfileOverlay createExecutionProfileOverlay() {
    List<ProfileOverlay.KVPair> overlayPairs = new ArrayList<>();

    // Set worker class
    overlayPairs.add(new ProfileOverlay.KVPair(
        GobblinTemporalConfigurationKeys.WORKER_CLASS,
        "org.apache.gobblin.temporal.ddm.worker.ExecutionWorker"
    ));

    // Set Helix tag
    overlayPairs.add(new ProfileOverlay.KVPair(
        GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY,
        "execution"
    ));

    // Set stage-specific memory (execution operations are typically memory-intensive)
    // Falls back to global CONTAINER_MEMORY_MBS_KEY if stage-specific memory not configured
    if (this.config.hasPath(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB)) {
      String executionMemoryMb = this.config.getString(
          GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB);
      overlayPairs.add(new ProfileOverlay.KVPair(
          GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
          executionMemoryMb
      ));
      log.info("Setting execution container memory to {} MB", executionMemoryMb);
    }

    return new ProfileOverlay.Adding(overlayPairs);
  }

  /**
   * Handle the completion of a container. A new container will be requested to replace the one
   * that just exited depending on the exit status.
   * <p>
   * A container completes in either of the following conditions:
   * <ol>
   *   <li> The container gets stopped by the ApplicationMaster. </li>
   *   <li> Some error happens in the container and caused the container to exit </li>
   *   <li> The container gets preempted by the ResourceManager </li>
   *   <li> The container gets killed due to some reason, for example, if it runs over the allowed amount of virtual or physical memory </li>
   * </ol>
   * A replacement container is needed in all except the first case.
   * </p>
   */
  @Override
  protected void handleContainerCompletion(ContainerStatus containerStatus) {
    ContainerId completedContainerId = containerStatus.getContainerId();
    ContainerInfo completedContainerInfo = this.containerMap.remove(completedContainerId);

    // Because callbacks are processed asynchronously, we might encounter situations where handleContainerCompletion()
    // is called before onContainersAllocated(), resulting in the containerId missing from the containersMap.
    // We use removedContainerIds to remember these containers and remove them from containerMap later
    // when we call reviseWorkforcePlanAndRequestNewContainers method
    if (completedContainerInfo == null) {
      log.warn("Container {} not found in containerMap. This container onContainersCompleted() likely called before onContainersAllocated()",
          completedContainerId);
      this.removedContainerIds.add(completedContainerId);
      return;
    }

    log.info("Container {} running profile {} completed with exit status {}",
        completedContainerId, completedContainerInfo.getWorkerProfileName(), containerStatus.getExitStatus());

    if (StringUtils.isNotBlank(containerStatus.getDiagnostics())) {
      log.info("Container {} running profile {} completed with diagnostics: {}",
          completedContainerId, completedContainerInfo.getWorkerProfileName(), containerStatus.getDiagnostics());
    }

    if (this.shutdownInProgress) {
      log.info("Ignoring container completion for container {} as shutdown is in progress", completedContainerId);
      return;
    }

    WorkerProfile workerProfile = completedContainerInfo.getWorkerProfile();

    switch (containerStatus.getExitStatus()) {
      case(ContainerExitStatus.ABORTED):
        handleAbortedContainer(completedContainerId, completedContainerInfo);
        break;
      case(ContainerExitStatus.PREEMPTED):
        log.info("Container {} for profile {} preempted, starting to launching a replacement container",
            completedContainerId, completedContainerInfo.getWorkerProfileName());
        requestContainersForWorkerProfile(workerProfile, 1);
        break;
      case(GENERAL_OOM_EXIT_STATUS_CODE):
      case(ContainerExitStatus.KILLED_EXCEEDED_VMEM):
      case(ContainerExitStatus.KILLED_EXCEEDED_PMEM):
        handleContainerExitedWithOOM(completedContainerId, completedContainerInfo);
        break;
      case(LAUNCH_CONTAINER_FAILED_EXIT_CODE):
        log.info("Exit status 1.CompletedContainerInfo = {}", completedContainerInfo);
        break;
      case ContainerExitStatus.KILLED_AFTER_APP_COMPLETION:
      case ContainerExitStatus.SUCCESS:
        break;
      default:
        log.warn("Container {} exited with unhandled status code {}. ContainerInfo: {}",
            completedContainerId, containerStatus.getExitStatus(), completedContainerInfo);
        break;
    }
  }

  /**
   * Revises the workforce plan and requests new containers based on the given scaling directives.
   *
   * @param scalingDirectives the list of scaling directives
   */
  public synchronized void reviseWorkforcePlanAndRequestNewContainers(List<ScalingDirective> scalingDirectives) {
    if (CollectionUtils.isEmpty(scalingDirectives)) {
      return;
    }
    this.workforcePlan.reviseWhenNewer(scalingDirectives);
    calcDeltasAndRequestContainers();
  }

  public synchronized void calcDeltasAndRequestContainers() {
    // Correct the actualWorkforceStaffing in case of handleContainerCompletion() getting called before onContainersAllocated()
    Iterator<ContainerId> iterator = removedContainerIds.iterator();
    while (iterator.hasNext()) {
      ContainerId containerId = iterator.next();
      ContainerInfo containerInfo = this.containerMap.remove(containerId);
      if (containerInfo != null) {
        WorkerProfile workerProfile = containerInfo.getWorkerProfile();
        int currNumContainers = this.actualWorkforceStaffing.getStaffing(workerProfile.getName()).orElse(0);
        if (currNumContainers > 0) {
          this.actualWorkforceStaffing.reviseStaffing(workerProfile.getName(), currNumContainers - 1,
              System.currentTimeMillis());
        }
        iterator.remove();
      }
    }
    StaffingDeltas deltas = this.workforcePlan.calcStaffingDeltas(this.actualWorkforceStaffing);
    requestNewContainersForStaffingDeltas(deltas);
  }

  private synchronized void requestNewContainersForStaffingDeltas(StaffingDeltas deltas) {
    deltas.getPerProfileDeltas().forEach(profileDelta -> {
      WorkerProfile workerProfile = profileDelta.getProfile();
      String profileName = workerProfile.getName();
      int delta = profileDelta.getDelta();
      int currNumContainers = this.actualWorkforceStaffing.getStaffing(profileName).orElse(0);
      if (delta > 0) { // scale up!
        log.info("Requesting {} new containers for profile {} having currently {} containers", delta,
            WorkforceProfiles.renderName(profileName), currNumContainers);
        requestContainersForWorkerProfile(workerProfile, delta);
        // update our staffing after requesting new containers
        this.actualWorkforceStaffing.reviseStaffing(profileName, currNumContainers + delta, System.currentTimeMillis());
      } else if (delta < 0) { // scale down!
        log.info("Releasing {} containers for profile {} having currently {} containers", -delta,
            WorkforceProfiles.renderName(profileName), currNumContainers);
        releaseContainersForWorkerProfile(profileName, delta);
        // update our staffing after releasing containers
        int numContainersAfterRelease = Math.max(currNumContainers + delta, 0);
        this.actualWorkforceStaffing.reviseStaffing(profileName, numContainersAfterRelease, System.currentTimeMillis());
      } // else, already at staffing plan (or at least have requested, so in-progress)
    });
  }

  private void handleAbortedContainer(ContainerId completedContainerId, ContainerInfo completedContainerInfo) {
    // Case 1 : Container release requested while scaling down
    if (this.releasedContainerCache.getIfPresent(completedContainerId) != null) {
      log.info("Container {} was released while downscaling for profile {}", completedContainerId, completedContainerInfo.getWorkerProfileName());
      this.releasedContainerCache.invalidate(completedContainerId);
      return;
    }

    // Case 2 : Container release was not requested, we need to request a replacement container
    log.info("Container {} aborted for profile {}, starting to launch a replacement container", completedContainerId, completedContainerInfo.getWorkerProfileName());
    requestContainersForWorkerProfile(completedContainerInfo.getWorkerProfile(), 1);
  }

  private synchronized void handleContainerExitedWithOOM(ContainerId completedContainerId, ContainerInfo completedContainerInfo) {
    WorkerProfile workerProfile = completedContainerInfo.getWorkerProfile();

    // Determine which workflow stage this container belongs to
    WorkflowStage stage = WorkflowStage.fromProfileName(workerProfile.getName());

    log.info("Container {} for profile {} (stage: {}) exited with OOM, starting to launch a replacement container",
        completedContainerId, completedContainerInfo.getWorkerProfileName(), stage);

    List<ScalingDirective> scalingDirectives = new ArrayList<>();

    long currTimeMillis = System.currentTimeMillis();
    // Update the current staffing to reflect the container that exited with OOM
    int currNumContainers = this.actualWorkforceStaffing.getStaffing(workerProfile.getName()).orElse(0);
    if (currNumContainers > 0) {
      this.actualWorkforceStaffing.reviseStaffing(workerProfile.getName(), currNumContainers - 1, currTimeMillis);
      // Add a scaling directive so that workforcePlan have uptodate setPoints for the workerProfile,
      // otherwise extra containers will be requested when calculating deltas
      scalingDirectives.add(new ScalingDirective(workerProfile.getName(), currNumContainers - 1, currTimeMillis));
    }

    // Get stage-specific OOM configuration
    int memoryMultiplier = getMemoryMultiplierForStage(stage);
    int maxMemoryMbs = getMaxMemoryForStage(stage);

    // Request a replacement container with stage-specific limits
    int currContainerMemoryMbs = workerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    if (currContainerMemoryMbs >= maxMemoryMbs) {
      log.warn("Container {} for stage {} already had max allowed memory {} MBs (stage max: {} MBs). Not requesting a replacement container.",
          completedContainerId, stage, currContainerMemoryMbs, maxMemoryMbs);
      return;
    }
    int newContainerMemoryMbs = Math.min(currContainerMemoryMbs * memoryMultiplier, maxMemoryMbs);

    log.info("Creating OOM replacement for stage {} with memory: {} MB -> {} MB (max: {} MB)",
        stage, currContainerMemoryMbs, newContainerMemoryMbs, maxMemoryMbs);

    // Derive from global baseline with stage-specific overlays
    ProfileOverlay overlay = createStageSpecificOOMOverlay(stage, newContainerMemoryMbs);
    Optional<ProfileDerivation> optProfileDerivation = Optional.of(
        new ProfileDerivation(WorkforceProfiles.BASELINE_NAME, overlay)
    );
    scalingDirectives.add(new ScalingDirective(
        stage.getProfileBaseName() + "-oomReplacement-" + profileNameSuffixGenerator.getAndIncrement(),
        1,
        currTimeMillis + EPSILON_MIILIS, // Each scaling directive should have a newer timestamp than the previous one
        optProfileDerivation
    ));
    reviseWorkforcePlanAndRequestNewContainers(scalingDirectives);
  }

  /**
   * Gets the memory multiplier for OOM retries for a specific workflow stage.
   */
  private int getMemoryMultiplierForStage(WorkflowStage stage) {
    String key;
    int defaultValue;

    switch (stage) {
      case WORK_DISCOVERY:
      case COMMIT:
        key = GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_OOM_MEMORY_MULTIPLIER;
        defaultValue = GobblinTemporalConfigurationKeys.DEFAULT_DISCOVERY_COMMIT_OOM_MEMORY_MULTIPLIER;
        break;
      case WORK_EXECUTION:
        key = GobblinTemporalConfigurationKeys.WORK_EXECUTION_OOM_MEMORY_MULTIPLIER;
        defaultValue = GobblinTemporalConfigurationKeys.DEFAULT_WORK_EXECUTION_OOM_MEMORY_MULTIPLIER;
        break;
      default:
        return DEFAULT_REPLACEMENT_CONTAINER_MEMORY_MULTIPLIER;
    }

    return config.hasPath(key) ? config.getInt(key) : defaultValue;
  }

  /**
   * Gets the maximum memory allowed for OOM retries for a specific workflow stage.
   */
  private int getMaxMemoryForStage(WorkflowStage stage) {
    String key;
    int defaultValue;

    switch (stage) {
      case WORK_DISCOVERY:
      case COMMIT:
        key = GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_OOM_MAX_MEMORY_MB;
        defaultValue = GobblinTemporalConfigurationKeys.DEFAULT_DISCOVERY_COMMIT_OOM_MAX_MEMORY_MB;
        break;
      case WORK_EXECUTION:
        key = GobblinTemporalConfigurationKeys.WORK_EXECUTION_OOM_MAX_MEMORY_MB;
        defaultValue = GobblinTemporalConfigurationKeys.DEFAULT_WORK_EXECUTION_OOM_MAX_MEMORY_MB;
        break;
      default:
        return MAX_REPLACEMENT_CONTAINER_MEMORY_MBS;
    }

    return config.hasPath(key) ? config.getInt(key) : defaultValue;
  }

  /**
   * Creates a ProfileOverlay for OOM replacement with stage-specific memory and worker class.
   * This derives from the global baseline, ensuring task queue routing is preserved.
   */
  private ProfileOverlay createStageSpecificOOMOverlay(WorkflowStage stage, int newMemoryMbs) {
    List<ProfileOverlay.KVPair> overlayPairs = new ArrayList<>();

    // Add increased memory
    overlayPairs.add(new ProfileOverlay.KVPair(
        GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
        String.valueOf(newMemoryMbs)
    ));

    // Add stage-specific worker class to ensure correct task queue routing
    String workerClass = getWorkerClassForStage(stage);
    overlayPairs.add(new ProfileOverlay.KVPair(
        GobblinTemporalConfigurationKeys.WORKER_CLASS,
        workerClass
    ));

    return new ProfileOverlay.Adding(overlayPairs);
  }

  /**
   * Gets the worker class for a specific workflow stage.
   */
  private String getWorkerClassForStage(WorkflowStage stage) {
    switch (stage) {
      case WORK_DISCOVERY:
        return "org.apache.gobblin.temporal.ddm.worker.DiscoveryCommitWorker";
      case WORK_EXECUTION:
        return "org.apache.gobblin.temporal.ddm.worker.ExecutionWorker";
      case COMMIT:
        return "org.apache.gobblin.temporal.ddm.worker.DiscoveryCommitWorker";
      default:
        return "org.apache.gobblin.temporal.ddm.worker.WorkFulfillmentWorker";
    }
  }

}
