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
package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.util.Map;
import java.util.Optional;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

import com.typesafe.config.ConfigFactory;

import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.temporal.cluster.WorkerConfig;
import org.apache.gobblin.temporal.ddm.util.TemporalWorkFlowUtils;
import org.apache.gobblin.temporal.ddm.work.CommitStats;
import org.apache.gobblin.temporal.ddm.work.EagerFsDirBackedWorkUnitClaimCheckWorkload;
import org.apache.gobblin.temporal.util.nesting.work.NestingExecWorkloadInput;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.ddm.work.styles.FileSystemJobStateful;
import org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow;
import org.apache.gobblin.temporal.ddm.workflow.ProcessWorkUnitsWorkflow;
import org.apache.gobblin.temporal.util.nesting.work.WorkflowAddr;
import org.apache.gobblin.temporal.util.nesting.work.Workload;
import org.apache.gobblin.temporal.util.nesting.workflow.NestingExecWorkflow;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.temporal.workflows.metrics.EventTimer;
import org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer;


@Slf4j
public class ProcessWorkUnitsWorkflowImpl implements ProcessWorkUnitsWorkflow {
  public static final String CHILD_WORKFLOW_ID_BASE = "NestingExecWorkUnits";
  public static final String COMMIT_STEP_WORKFLOW_ID_BASE = "CommitStepWorkflow";

  @Override
  public CommitStats process(WUProcessingSpec workSpec, final Properties props) {
    Optional<EventTimer> timer = this.createOptJobEventTimer(workSpec);
    CommitStats result = performWork(workSpec, props);
    timer.ifPresent(EventTimer::stop);
    return result;
  }

  private CommitStats performWork(WUProcessingSpec workSpec, final Properties props) {
    Workload<WorkUnitClaimCheck> workload = createWorkload(workSpec);
    Map<String, Object> searchAttributes = TemporalWorkFlowUtils.generateGaasSearchAttributes(props);
    NestingExecWorkflow<WorkUnitClaimCheck> processingWorkflow = createProcessingWorkflow(workSpec, searchAttributes);

    Optional<Integer> workunitsProcessed = Optional.empty();
    try {
      NestingExecWorkloadInput<WorkUnitClaimCheck>
          performWorkloadInput = new NestingExecWorkloadInput<>(WorkflowAddr.ROOT, workload, 0,
          workSpec.getTuning().getMaxBranchesPerTree(), workSpec.getTuning().getMaxSubTreesPerTree(),
          Optional.empty(), props);
      workunitsProcessed = Optional.of(processingWorkflow.performWorkload(performWorkloadInput));
    } catch (Exception e) {
      log.error("ProcessWorkUnits failure - attempting partial commit before re-throwing exception", e);

      try {
        performCommitIfAnyWorkUnitsProcessed(workSpec, searchAttributes, workunitsProcessed, props);// Attempt partial commit before surfacing the failure
      } catch (Exception commitException) {
        // Combine current and commit exception messages for a more complete context
        String combinedMessage = String.format(
            "Processing failure: %s. Commit workflow failure: %s",
            e.getMessage(),
            commitException.getMessage()
        );
        log.error(combinedMessage);
        throw ApplicationFailure.newNonRetryableFailureWithCause(
            String.format("Processing failure: %s. Partial commit failure: %s", combinedMessage, commitException),
            Exception.class.toString(),
            new Exception(e)); // Wrap the original exception for stack trace preservation
      }
      throw e;// Re-throw after any partial commit, to fail the parent workflow in case commitWorkflow didn't flow (unlikely)
    }
    return performCommitIfAnyWorkUnitsProcessed(workSpec, searchAttributes, workunitsProcessed, props);
  }

  private CommitStats performCommitIfAnyWorkUnitsProcessed(WUProcessingSpec workSpec,
      Map<String, Object> searchAttributes, Optional<Integer> workunitsProcessed, Properties props) {
    //  we are only inhibiting commit when workunitsProcessed is actually known to be zero
    if (workunitsProcessed.filter(n -> n == 0).isPresent()) {
      log.error("No work units processed, so no commit attempted.");
      return CommitStats.createEmpty();
    }
    CommitStepWorkflow commitWorkflow = createCommitStepWorkflow(searchAttributes);
    CommitStats result = commitWorkflow.commit(workSpec, props);
    if (result.getNumCommittedWorkUnits() == 0) {
      log.warn("No work units committed at the job level. They could have been committed at the task level.");
    }
    return result;
  }

  private Optional<EventTimer> createOptJobEventTimer(WUProcessingSpec workSpec) {
    if (workSpec.isToDoJobLevelTiming()) {
      EventSubmitterContext eventSubmitterContext = workSpec.getEventSubmitterContext();
      TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.WithinWorkflowFactory(eventSubmitterContext);
      return Optional.of(timerFactory.createJobTimer());
    } else {
      return Optional.empty();
    }
  }

  protected Workload<WorkUnitClaimCheck> createWorkload(WUProcessingSpec workSpec) {
    return new EagerFsDirBackedWorkUnitClaimCheckWorkload(workSpec.getFileSystemUri(), workSpec.getWorkUnitsDir(),
        workSpec.getEventSubmitterContext());
  }

  protected NestingExecWorkflow<WorkUnitClaimCheck> createProcessingWorkflow(FileSystemJobStateful f,
      Map<String, Object> searchAttributes) {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE)
        .setSearchAttributes(searchAttributes)
        .setWorkflowId(Help.qualifyNamePerExecWithFlowExecId(CHILD_WORKFLOW_ID_BASE, f,
            WorkerConfig.of(this).orElse(ConfigFactory.empty())))
        .build();
    // TODO: to incorporate multiple different concrete `NestingExecWorkflow` sub-workflows in the same super-workflow... shall we use queues?!?!?
    return Workflow.newChildWorkflowStub(NestingExecWorkflow.class, childOpts);
  }

  protected CommitStepWorkflow createCommitStepWorkflow(Map<String, Object> searchAttributes) {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        // TODO: verify to instead use:  Policy.PARENT_CLOSE_POLICY_TERMINATE)
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
        .setSearchAttributes(searchAttributes)
        .setWorkflowId(Help.qualifyNamePerExecWithFlowExecId(COMMIT_STEP_WORKFLOW_ID_BASE,
            WorkerConfig.of(this).orElse(ConfigFactory.empty())))
        .build();

    return Workflow.newChildWorkflowStub(CommitStepWorkflow.class, childOpts);
  }
}
