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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

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
  private static final int GENERAL_OOM_EXIT_STATUS_CODE = 137;
  private static final int DEFAULT_REPLACEMENT_CONTAINER_MEMORY_MULTIPLIER = 2;
  private static final int MAX_REPLACEMENT_CONTAINER_MEMORY_MBS = 65536; // 64GB

  /** this holds the current count of containers already requested for each worker profile */
  private final WorkforceStaffing actualWorkforceStaffing;
  /** this holds the current total workforce plan as per latest received scaling directives */
  private final WorkforcePlan workforcePlan;
  private final Set<ContainerId> removedContainerIds;
  private final AtomicLong profileNameSuffixGenerator;

  public DynamicScalingYarnService(Config config, String applicationName, String applicationId,
      YarnConfiguration yarnConfiguration, FileSystem fs, EventBus eventBus) throws Exception {
    super(config, applicationName, applicationId, yarnConfiguration, fs, eventBus);

    this.actualWorkforceStaffing = WorkforceStaffing.initialize(0);
    this.workforcePlan = new WorkforcePlan(this.config, this.config.getInt(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY));
    this.removedContainerIds = ConcurrentHashMap.newKeySet();
    this.profileNameSuffixGenerator = new AtomicLong();
  }

  @Override
  protected synchronized void requestInitialContainers() {
    StaffingDeltas deltas = this.workforcePlan.calcStaffingDeltas(this.actualWorkforceStaffing);
    requestNewContainersForStaffingDeltas(deltas);
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
          // Add a scaling directive so that workforcePlan have uptodate setPoints for the workerProfile,
          // otherwise extra containers will be requested when calculating deltas
          scalingDirectives.add(new ScalingDirective(workerProfile.getName(), currNumContainers - 1, System.currentTimeMillis()));
        }
        iterator.remove();
      }
    }

    this.workforcePlan.reviseWhenNewer(scalingDirectives);
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
    log.info("Container {} for profile {} exited with OOM, starting to launch a replacement container",
        completedContainerId, completedContainerInfo.getWorkerProfileName());

    List<ScalingDirective> scalingDirectives = new ArrayList<>();

    WorkerProfile workerProfile = completedContainerInfo.getWorkerProfile();
    // Update the current staffing to reflect the container that exited with OOM
    int currNumContainers = this.actualWorkforceStaffing.getStaffing(workerProfile.getName()).orElse(0);
    if (currNumContainers > 0) {
      this.actualWorkforceStaffing.reviseStaffing(workerProfile.getName(), currNumContainers - 1, System.currentTimeMillis());
      // Add a scaling directive so that workforcePlan have uptodate setPoints for the workerProfile,
      // otherwise extra containers will be requested when calculating deltas
      scalingDirectives.add(new ScalingDirective(workerProfile.getName(), currNumContainers - 1, System.currentTimeMillis()));
    }

    // Request a replacement container
    int currContainerMemoryMbs = workerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    int newContainerMemoryMbs = currContainerMemoryMbs * DEFAULT_REPLACEMENT_CONTAINER_MEMORY_MULTIPLIER;
    if (currContainerMemoryMbs < MAX_REPLACEMENT_CONTAINER_MEMORY_MBS && newContainerMemoryMbs > MAX_REPLACEMENT_CONTAINER_MEMORY_MBS) {
      newContainerMemoryMbs = MAX_REPLACEMENT_CONTAINER_MEMORY_MBS;
    } else if (newContainerMemoryMbs > MAX_REPLACEMENT_CONTAINER_MEMORY_MBS) {
      log.warn("Expected replacement container memory exceeds the maximum allowed memory {}. Not requesting a replacement container.",
          MAX_REPLACEMENT_CONTAINER_MEMORY_MBS);
      return;
    }
    Optional<ProfileDerivation> optProfileDerivation = Optional.of(new ProfileDerivation(workerProfile.getName(),
        new ProfileOverlay.Adding(new ProfileOverlay.KVPair(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY, newContainerMemoryMbs + ""))
    ));
    scalingDirectives.add(new ScalingDirective(
        DEFAULT_REPLACEMENT_CONTAINER_WORKER_PROFILE_NAME_PREFIX + "-" + profileNameSuffixGenerator.getAndIncrement(),
        1,
        System.currentTimeMillis(),
        optProfileDerivation
    ));
    reviseWorkforcePlanAndRequestNewContainers(scalingDirectives);
  }

}
