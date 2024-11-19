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

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.StaffingDeltas;
import org.apache.gobblin.temporal.dynamic.WorkerProfile;
import org.apache.gobblin.temporal.dynamic.WorkforcePlan;
import org.apache.gobblin.temporal.dynamic.WorkforceStaffing;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;

@Slf4j
public class DynamicScalingYarnService extends YarnService {

  private final WorkforceStaffing workforceStaffing;
  private final WorkforcePlan workforcePlan;

  public DynamicScalingYarnService(Config config, String applicationName, String applicationId,
      YarnConfiguration yarnConfiguration, FileSystem fs, EventBus eventBus) throws Exception {
    super(config, applicationName, applicationId, yarnConfiguration, fs, eventBus);

    this.workforceStaffing = WorkforceStaffing.initialize(getInitialContainers());
    this.workforcePlan = new WorkforcePlan(getConfig(), getInitialContainers());
  }

  public synchronized void reviseWorkforcePlanAndRequestNewContainers(List<ScalingDirective> scalingDirectives) {
    this.workforcePlan.reviseWhenNewer(scalingDirectives);
    StaffingDeltas deltas = this.workforcePlan.calcStaffingDeltas(this.workforceStaffing);
    requestNewContainersForStaffingDeltas(deltas);
    // update our staffing after requesting new containers
    scalingDirectives.forEach(directive -> this.workforceStaffing.reviseStaffing(
        directive.getProfileName(), directive.getSetPoint(), directive.getTimestampEpochMillis())
    );
  }

  private synchronized void requestNewContainersForStaffingDeltas(StaffingDeltas deltas) {
    deltas.getPerProfileDeltas().forEach(profileDelta -> {
      if (profileDelta.getDelta() > 0) {
        log.info("Requesting {} new containers for profile {}", profileDelta.getDelta(), profileDelta.getProfile());
        requestContainersForWorkerProfile(profileDelta.getProfile(), profileDelta.getDelta());
      }
      // TODO: Decide how to handle negative deltas
    });
  }

  private synchronized void requestContainersForWorkerProfile(WorkerProfile workerProfile, int numContainers) {
    int containerMemoryMbs = workerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    int containerCores = workerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY);
    long allocationRequestId = generateAllocationRequestId(workerProfile);
    requestContainers(numContainers, Resource.newInstance(containerMemoryMbs, containerCores), Optional.of(allocationRequestId));
  }

}
