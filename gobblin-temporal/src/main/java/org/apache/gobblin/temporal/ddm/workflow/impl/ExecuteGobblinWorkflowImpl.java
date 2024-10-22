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
import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.ConfigFactory;

import io.temporal.activity.ActivityOptions;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.ddm.activity.DeleteWorkDirsActivity;
import org.apache.gobblin.temporal.ddm.activity.GenerateWorkUnits;
import org.apache.gobblin.temporal.ddm.launcher.ProcessWorkUnitsJobLauncher;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.CommitStats;
import org.apache.gobblin.temporal.ddm.work.DirDeletionResult;
import org.apache.gobblin.temporal.ddm.work.ExecGobblinStats;
import org.apache.gobblin.temporal.ddm.work.GenerateWorkUnitsResult;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.ddm.workflow.ExecuteGobblinWorkflow;
import org.apache.gobblin.temporal.ddm.workflow.ProcessWorkUnitsWorkflow;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.temporal.workflows.metrics.EventTimer;
import org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer;
import org.apache.gobblin.temporal.ddm.util.TemporalWorkFlowUtils;
import org.apache.gobblin.util.PropertiesUtils;


@Slf4j
public class ExecuteGobblinWorkflowImpl implements ExecuteGobblinWorkflow {
  public static final String PROCESS_WORKFLOW_ID_BASE = "ProcessWorkUnits";

  public static final Duration genWUsStartToCloseTimeout = Duration.ofHours(2); // TODO: make configurable... also add activity heartbeats

  private static final RetryOptions GEN_WUS_ACTIVITY_RETRY_OPTS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  private static final ActivityOptions GEN_WUS_ACTIVITY_OPTS = ActivityOptions.newBuilder()
      .setStartToCloseTimeout(genWUsStartToCloseTimeout)
      .setRetryOptions(GEN_WUS_ACTIVITY_RETRY_OPTS)
      .build();

  private final GenerateWorkUnits genWUsActivityStub = Workflow.newActivityStub(GenerateWorkUnits.class,
      GEN_WUS_ACTIVITY_OPTS);

  private static final RetryOptions DELETE_WORK_DIRS_RETRY_OPTS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  private static final ActivityOptions DELETE_WORK_DIRS_ACTIVITY_OPTS = ActivityOptions.newBuilder()
      .setStartToCloseTimeout(Duration.ofHours(1))
      .setRetryOptions(DELETE_WORK_DIRS_RETRY_OPTS)
      .build();
  private final DeleteWorkDirsActivity deleteWorkDirsActivityStub = Workflow.newActivityStub(DeleteWorkDirsActivity.class, DELETE_WORK_DIRS_ACTIVITY_OPTS);

  @Override
  public ExecGobblinStats execute(Properties jobProps, EventSubmitterContext eventSubmitterContext) {
    TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.Factory(eventSubmitterContext);
    timerFactory.create(TimingEvent.LauncherTimings.JOB_PREPARE).submit();
    EventTimer timer = timerFactory.createJobTimer();
    Optional<GenerateWorkUnitsResult> generateWorkUnitResultsOpt = Optional.empty();
    WUProcessingSpec wuSpec = createProcessingSpec(jobProps, eventSubmitterContext);
    boolean isSuccessful = false;
    try {
      generateWorkUnitResultsOpt = Optional.of(genWUsActivityStub.generateWorkUnits(jobProps, eventSubmitterContext));
      int numWUsGenerated = generateWorkUnitResultsOpt.get().getGeneratedWuCount();
      int numWUsCommitted = 0;
      CommitStats commitStats = CommitStats.createEmpty();
      if (numWUsGenerated > 0) {
        ProcessWorkUnitsWorkflow processWUsWorkflow = createProcessWorkUnitsWorkflow(jobProps);
        commitStats = processWUsWorkflow.process(wuSpec);
        numWUsCommitted = commitStats.getNumCommittedWorkUnits();
      }
      timer.stop();
      isSuccessful = true;
      return new ExecGobblinStats(numWUsGenerated, numWUsCommitted, jobProps.getProperty(Help.USER_TO_PROXY_KEY),
          commitStats.getDatasetStats());
    } catch (Exception e) {
      // Emit a failed GobblinTrackingEvent to record job failures
      timerFactory.create(TimingEvent.LauncherTimings.JOB_FAILED).submit();
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed Gobblin job %s", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)),
          e.getClass().getName(),
          e
      );
    } finally {
      // TODO: Cleanup WorkUnit/Taskstate Directory for jobs cancelled mid flight
      try {
        log.info("Cleaning up work dirs for job {}", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
        if (generateWorkUnitResultsOpt.isPresent()) {
          cleanupWorkDirs(wuSpec, eventSubmitterContext, generateWorkUnitResultsOpt.get().getWorkDirPathsToDelete());
        } else {
          log.warn("Skipping cleanup of work dirs for job due to no output from GenerateWorkUnits");
        }
      } catch (IOException e) {
        // Only fail the job with a new failure if the job was successful, otherwise keep the original error
        if (isSuccessful) {
          throw ApplicationFailure.newNonRetryableFailureWithCause(
              String.format("Failed cleaning Gobblin job %s", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)),
              e.getClass().getName(),
              e
          );
        }
        log.error("Failed to cleanup work dirs", e);
      }
    }
  }

  protected ProcessWorkUnitsWorkflow createProcessWorkUnitsWorkflow(Properties jobProps) {
    ChildWorkflowOptions childOpts = ChildWorkflowOptions.newBuilder()
        .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE)
        .setWorkflowId(Help.qualifyNamePerExecWithFlowExecId(PROCESS_WORKFLOW_ID_BASE, ConfigFactory.parseProperties(jobProps)))
        .setSearchAttributes(TemporalWorkFlowUtils.generateGaasSearchAttributes(jobProps))
        .build();
    return Workflow.newChildWorkflowStub(ProcessWorkUnitsWorkflow.class, childOpts);
  }

  protected static WUProcessingSpec createProcessingSpec(Properties jobProps, EventSubmitterContext eventSubmitterContext) {
    JobState jobState = new JobState(jobProps);
    URI fileSystemUri = JobStateUtils.getFileSystemUri(jobState);
    Path workUnitsDirPath = JobStateUtils.getWorkUnitsPath(jobState);
    WUProcessingSpec wuSpec = new WUProcessingSpec(fileSystemUri, workUnitsDirPath.toString(), eventSubmitterContext);
    // TODO: use our own prop names; don't "borrow" from `ProcessWorkUnitsJobLauncher`
    if (jobProps.containsKey(ProcessWorkUnitsJobLauncher.GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_BRANCHES_PER_TREE)
        && jobProps.containsKey(ProcessWorkUnitsJobLauncher.GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_SUB_TREES_PER_TREE)) {
      int maxBranchesPerTree = PropertiesUtils.getRequiredPropAsInt(jobProps, ProcessWorkUnitsJobLauncher.GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_BRANCHES_PER_TREE);
      int maxSubTreesPerTree = PropertiesUtils.getRequiredPropAsInt(jobProps, ProcessWorkUnitsJobLauncher.GOBBLIN_TEMPORAL_JOB_LAUNCHER_ARG_WORK_MAX_SUB_TREES_PER_TREE);
      wuSpec.setTuning(new WUProcessingSpec.Tuning(maxBranchesPerTree, maxSubTreesPerTree));
    }
    return wuSpec;
  }

  private void cleanupWorkDirs(WUProcessingSpec workSpec, EventSubmitterContext eventSubmitterContext, Set<String> directoriesToClean)
      throws IOException {
    // TODO: Add configuration to support cleaning up historical work dirs from same job name
    FileSystem fs = Help.loadFileSystem(workSpec);
    JobState jobState = Help.loadJobState(workSpec, fs);
    // TODO: Avoid cleaning up if work is being checkpointed e.g. midway of a commit for EXACTLY_ONCE

    if (PropertiesUtils.getPropAsBoolean(jobState.getProperties(), ConfigurationKeys.CLEANUP_STAGING_DATA_BY_INITIALIZER, "false")) {
      log.info("Skipping cleanup of work dirs for job due to initializer handling the cleanup");
      return;
    }

    DirDeletionResult dirDeletionResult = deleteWorkDirsActivityStub.delete(workSpec, eventSubmitterContext,
        calculateWorkDirsToDelete(jobState.getJobId(), directoriesToClean));

    for (String dir : directoriesToClean) {
      if (!dirDeletionResult.getSuccessesByDirPath().get(dir)) {
        throw new IOException("Unable to delete one of more directories in the DeleteWorkDirsActivity. Please clean up manually.");
      }
    }
  }

  protected static Set<String> calculateWorkDirsToDelete(String jobId, Set<String> workDirsToClean) throws IOException {
    // Only delete directories that are associated with the current job, otherwise
    Set<String> resultSet = new HashSet<>();
    Set<String> nonJobDirs = new HashSet<>();
    for (String dir : workDirsToClean) {
      if (dir.contains(jobId)) {
        resultSet.add(dir);
      } else {
        log.warn("Skipping deletion of directory {} as it is not associated with job {}", dir, jobId);
        nonJobDirs.add(dir);
      }
    }
    if (!nonJobDirs.isEmpty()) {
      throw new IOException("Found directories set to delete not associated with job " + jobId + ": " + nonJobDirs + ". Please validate staging and output directories");
    }
    return resultSet;
  }
}
