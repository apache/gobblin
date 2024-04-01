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

import java.util.Optional;

import com.typesafe.config.ConfigFactory;

import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.cluster.WorkerConfig;
import org.apache.gobblin.temporal.ddm.work.EagerFsDirBackedWorkUnitClaimCheckWorkload;
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
  public int process(WUProcessingSpec workSpec) {
    Optional<EventTimer> timer = this.createOptJobEventTimer(workSpec);
    int result = performWork(workSpec);
    timer.ifPresent(EventTimer::stop);
    return result;
  }

  private int performWork(WUProcessingSpec workSpec) {
    Workload<WorkUnitClaimCheck> workload = createWorkload(workSpec);
    NestingExecWorkflow<WorkUnitClaimCheck> processingWorkflow = createProcessingWorkflow(workSpec);
    int workunitsProcessed = processingWorkflow.performWorkload(
        WorkflowAddr.ROOT, workload, 0,
        workSpec.getTuning().getMaxBranchesPerTree(), workSpec.getTuning().getMaxSubTreesPerTree(), Optional.empty()
    );
    if (workunitsProcessed > 0) {
      CommitStepWorkflow commitWorkflow = createCommitStepWorkflow();
      int result = commitWorkflow.commit(workSpec);
      if (result == 0) {
        log.warn("No work units committed at the job level. They could have been committed at the task level.");
      }
      return result;
    } else {
      log.error("No work units processed, so no commit attempted.");
      return 0;
    }
  }

  private Optional<EventTimer> createOptJobEventTimer(WUProcessingSpec workSpec) {
    if (workSpec.isToDoJobLevelTiming()) {
      EventSubmitterContext eventSubmitterContext = workSpec.getEventSubmitterContext();
      TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.Factory(eventSubmitterContext);
      return Optional.of(timerFactory.createJobTimer());
    } else {
      return Optional.empty();
    }
  }

  protected Workload<WorkUnitClaimCheck> createWorkload(WUProcessingSpec workSpec) {
    return new EagerFsDirBackedWorkUnitClaimCheckWorkload(
        workSpec.getFileSystemUri(),
        workSpec.getWorkUnitsDir(),
        workSpec.getEventSubmitterContext()
    );
  }

  protected NestingExecWorkflow<WorkUnitClaimCheck> createProcessingWorkflow(FileSystemJobStateful f) {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE)
        .setWorkflowId(Help.qualifyNamePerExecWithFlowExecId(CHILD_WORKFLOW_ID_BASE, f, WorkerConfig.of(this).orElse(ConfigFactory.empty())))
        .build();
    // TODO: to incorporate multiple different concrete `NestingExecWorkflow` sub-workflows in the same super-workflow... shall we use queues?!?!?
    return Workflow.newChildWorkflowStub(NestingExecWorkflow.class, childOpts);
  }

  protected CommitStepWorkflow createCommitStepWorkflow() {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
        .setWorkflowId(Help.qualifyNamePerExecWithFlowExecId(COMMIT_STEP_WORKFLOW_ID_BASE, WorkerConfig.of(this).orElse(ConfigFactory.empty())))
        .build();

    return Workflow.newChildWorkflowStub(CommitStepWorkflow.class, childOpts);
  }
}
