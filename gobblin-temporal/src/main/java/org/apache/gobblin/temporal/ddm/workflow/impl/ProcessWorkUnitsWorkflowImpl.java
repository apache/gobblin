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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.hadoop.fs.FileSystem;

import com.typesafe.config.ConfigFactory;

import io.temporal.activity.ActivityOptions;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.util.JobMetrics;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
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
import org.apache.gobblin.temporal.workflows.timing.EventTimer;
import org.apache.gobblin.temporal.workflows.timing.TemporalEventTimer;
import org.apache.gobblin.temporal.workflows.trackingevent.activity.GobblinTrackingEventActivity;


@Slf4j
public class ProcessWorkUnitsWorkflowImpl implements ProcessWorkUnitsWorkflow {
  public static final String CHILD_WORKFLOW_ID_BASE = "NestingExecWorkUnits";
  public static final String COMMIT_STEP_WORKFLOW_ID_BASE = "CommitStepWorkflow";
  private final ActivityOptions options = ActivityOptions.newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(60))
      .build();
  private final GobblinTrackingEventActivity trackingEventActivity = Workflow.newActivityStub(
      GobblinTrackingEventActivity.class, options);

  private static final int MAX_DESERIALIZATION_FS_LOAD_ATTEMPTS = 5;

  @Override
  public int process(WUProcessingSpec workSpec, Properties props) {
    // NOTE: We are using the metrics tags from Job Props to create the metric context for the timer and NOT
    // the deserialized jobState from HDFS that is created by the real distcp job. This is because the AZ runtime
    // settings we want are for the job launcher that launched this Yarn job.
    State state = new State(props);

    try {
      FileSystem fs = Help.loadFileSystemForce(workSpec);
      JobState jobState = Help.loadJobStateUncached(workSpec, fs);
      List<Tag<?>> tags = getTags(state, jobState);
      EventSubmitter eventSubmitter = new EventSubmitter.Builder(
          Instrumented.getMetricContext(state, getClass(), tags),
          JobMetrics.NAMESPACE).build();

      TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.Factory(trackingEventActivity, eventSubmitter);
      try (EventTimer timer = timerFactory.getJobTimer()) {
        return performWork(workSpec);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int performWork(WUProcessingSpec workSpec) {
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
        log.warn("No work units committed at the job level. They could be committed at a task level.");
      }
      return result;
    } else {
      log.error("No work units processed, so no commit attempted.");
      return 0;
    }
  }

  protected Workload<WorkUnitClaimCheck> createWorkload(WUProcessingSpec workSpec) {
    return new EagerFsDirBackedWorkUnitClaimCheckWorkload(workSpec.getFileSystemUri(), workSpec.getWorkUnitsDir());
  }

  protected NestingExecWorkflow<WorkUnitClaimCheck> createProcessingWorkflow(FileSystemJobStateful f) {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
        .setWorkflowId(Help.qualifyNamePerExec(CHILD_WORKFLOW_ID_BASE, f, WorkerConfig.of(this).orElse(ConfigFactory.empty())))
        .build();
    // TODO: to incorporate multiple different concrete `NestingExecWorkflow` sub-workflows in the same super-workflow... shall we use queues?!?!?
    return Workflow.newChildWorkflowStub(NestingExecWorkflow.class, childOpts);
  }

  protected CommitStepWorkflow createCommitStepWorkflow() {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
        .setWorkflowId(Help.qualifyNamePerExec(COMMIT_STEP_WORKFLOW_ID_BASE, WorkerConfig.of(this).orElse(ConfigFactory.empty())))
        .build();

    return Workflow.newChildWorkflowStub(CommitStepWorkflow.class, childOpts);
  }

  private List<Tag<?>> getTags(State stateFromCurJob, JobState jobStateFromHdfs) {
    // Alternatively, the tags should also exist in the eventSubmitter in the job launcher
    List<Tag<?>> tagsFromCurJob = GobblinMetrics.getCustomTagsFromState(stateFromCurJob);
    // add tags jobState in the form of tags on HDFS and the rest of the fields will come from the current job
    Map<String, Tag<?>> tagsMap = new HashMap<>();
    Set<String> tagKeysFromJobState = new HashSet<>(Arrays.asList(
        TimingEvent.FlowEventConstants.FLOW_NAME_FIELD,
        TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD,
        TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        TimingEvent.FlowEventConstants.JOB_NAME_FIELD,
        TimingEvent.FlowEventConstants.JOB_GROUP_FIELD));

    // Step 1, Add tags from the AZ props using the original job (the one that launched this yarn app)
    tagsFromCurJob.forEach(tag -> tagsMap.put(tag.getKey(), tag));
    // Step 2. Add tags from the jobState (the original MR job on HDFS)
    GobblinMetrics.getCustomTagsFromState(jobStateFromHdfs).stream()
        .filter(tag -> tagKeysFromJobState.contains(tag.getKey()))
        .forEach(tag -> tagsMap.put(tag.getKey(), tag));

    // TEMP step: Add a suffix to the FLOW_NAME_FIELD AND FLOW_GROUP_FIELDS to prevent collisions with the original job
    String suffix = stateFromCurJob.getProp(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_JOB_METRICS_SUFFIX,
        GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_JOB_METRICS_SUFFIX);
    Consumer<String> addSuffix =  (key) -> tagsMap.put(key, new Tag<>(key, tagsMap.get(key).getValue() + suffix));
    addSuffix.accept(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD);
    addSuffix.accept(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD);

    // Step 3. Consolidate back into a list with no dupes
    return new ArrayList<>(tagsMap.values());
  }
}
