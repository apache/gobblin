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

package org.apache.gobblin.temporal.ddm.activity.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.activity.RecommendScalingForWorkUnits;
import org.apache.gobblin.temporal.ddm.work.TimeBudget;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;
import org.apache.gobblin.temporal.ddm.workflow.WorkflowStage;
import org.apache.gobblin.temporal.dynamic.ProfileDerivation;
import org.apache.gobblin.temporal.dynamic.ProfileOverlay;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


/**
 * Skeletal impl handling all foundational concerns, but leaving it to a concrete impl to actually choose the auto-scaling
 * {@link ScalingDirective#getSetPoint()} for the exactly one {@link ScalingDirective} being recommended.
 */
@Slf4j
public abstract class AbstractRecommendScalingForWorkUnitsImpl implements RecommendScalingForWorkUnits {

  // TODO: decide whether this name ought to be configurable - or instead a predictable name that callers may expect (and possibly adjust)
  public static final String DEFAULT_PROFILE_DERIVATION_NAME = "workUnitsProc";

  @Override
  public List<ScalingDirective> recommendScaling(WorkUnitsSizeSummary remainingWork, String sourceClass, TimeBudget timeBudget, Properties jobProps, WorkflowStage stage) {
    // NOTE: no attempt to determine the current scaling - per `RecommendScalingForWorkUnits` javadoc, the `ScalingDirective`(s) returned must "stand alone",
    // presuming nothing of the current `WorkforcePlan`'s `WorkforceStaffing`
    JobState jobState = new JobState(jobProps);
    ScalingDirective procWUsWorkerScaling = new ScalingDirective(
        calcProfileDerivationName(jobState, stage),
        calcDerivationSetPoint(remainingWork, sourceClass, timeBudget, jobState),
        System.currentTimeMillis(),
        Optional.of(calcProfileDerivation(calcBasisProfileName(jobState, stage), remainingWork, sourceClass, jobState, stage))
    );
    log.info("Recommended re-scaling for {} stage to process work units: {}", stage, procWUsWorkerScaling);
    return Arrays.asList(procWUsWorkerScaling);
  }

  protected abstract int calcDerivationSetPoint(WorkUnitsSizeSummary remainingWork, String sourceClass, TimeBudget timeBudget, JobState jobState);

  protected ProfileDerivation calcProfileDerivation(String basisProfileName, WorkUnitsSizeSummary remainingWork, String sourceClass, JobState jobState, WorkflowStage stage) {
    // Create overlay with stage-specific memory and worker class
    ProfileOverlay overlay = createStageSpecificOverlay(jobState, stage);
    return new ProfileDerivation(basisProfileName, overlay);
  }

  protected String calcProfileDerivationName(JobState jobState, WorkflowStage stage) {
    // TODO: if we ever return > 1 directive, append a monotonically increasing number to avoid collisions
    return stage.getProcessingProfileName();  // e.g., "workExecution-proc"
  }

  protected String calcBasisProfileName(JobState jobState, WorkflowStage stage) {
    // Always derive from the global baseline
    return WorkforceProfiles.BASELINE_NAME;
  }

  /**
   * Creates a ProfileOverlay with stage-specific memory and worker class configuration.
   * This allows deriving stage-specific profiles from the global baseline.
   */
  private ProfileOverlay createStageSpecificOverlay(JobState jobState, WorkflowStage stage) {
    List<ProfileOverlay.KVPair> overlayPairs = new java.util.ArrayList<>();

    // Add stage-specific memory if configured
    String memoryKey = getStageMemoryConfigKey(stage);
    if (jobState.contains(memoryKey)) {
      overlayPairs.add(new ProfileOverlay.KVPair(
          GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
          jobState.getProp(memoryKey)
      ));
    }

    // Add stage-specific worker class
    String workerClass = getWorkerClassForStage(stage);
    overlayPairs.add(new ProfileOverlay.KVPair(
        GobblinTemporalConfigurationKeys.WORKER_CLASS,
        workerClass
    ));

    return overlayPairs.isEmpty() ? ProfileOverlay.unchanged() : new ProfileOverlay.Adding(overlayPairs);
  }

  /**
   * Returns the configuration key for stage-specific memory.
   */
  private String getStageMemoryConfigKey(WorkflowStage stage) {
    switch (stage) {
      case WORK_DISCOVERY:
      case COMMIT:
        return GobblinTemporalConfigurationKeys.DISCOVERY_COMMIT_MEMORY_MB;
      case WORK_EXECUTION:
        return GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB;
      default:
        throw new IllegalArgumentException("Unknown stage: " + stage);
    }
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
