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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;

import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.failure.ApplicationFailure;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.cluster.GobblinClusterUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;
import org.apache.gobblin.temporal.ddm.activity.DeleteWorkDirsActivity;
import org.apache.gobblin.temporal.ddm.activity.GenerateWorkUnits;
import org.apache.gobblin.temporal.ddm.activity.RecommendScalingForWorkUnits;
import org.apache.gobblin.temporal.ddm.launcher.ProcessWorkUnitsJobLauncher;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.util.TemporalWorkFlowUtils;
import org.apache.gobblin.temporal.ddm.work.CommitStats;
import org.apache.gobblin.temporal.ddm.work.DirDeletionResult;
import org.apache.gobblin.temporal.ddm.work.ExecGobblinStats;
import org.apache.gobblin.temporal.ddm.work.GenerateWorkUnitsResult;
import org.apache.gobblin.temporal.ddm.work.TimeBudget;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.ddm.workflow.ExecuteGobblinWorkflow;
import org.apache.gobblin.temporal.ddm.workflow.ProcessWorkUnitsWorkflow;
import org.apache.gobblin.temporal.dynamic.FsScalingDirectivesRecipient;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.ScalingDirectivesRecipient;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.temporal.workflows.metrics.EventTimer;
import org.apache.gobblin.temporal.workflows.metrics.TemporalEventTimer;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


@Slf4j
public class ExecuteGobblinWorkflowImpl implements ExecuteGobblinWorkflow {
  public static final String PROCESS_WORKFLOW_ID_BASE = "ProcessWorkUnits";

  @Override
  public ExecGobblinStats execute(Properties jobProps, EventSubmitterContext eventSubmitterContext) {
    // Filtering only temporal job properties to pass to child workflows to avoid passing unnecessary properties
    final Properties temporalJobProps = PropertiesUtils.extractPropertiesWithPrefix(jobProps,
        com.google.common.base.Optional.of(GobblinTemporalConfigurationKeys.PREFIX));
    TemporalEventTimer.Factory timerFactory = new TemporalEventTimer.WithinWorkflowFactory(eventSubmitterContext, temporalJobProps);
    timerFactory.create(TimingEvent.LauncherTimings.JOB_PREPARE).submit(); // update GaaS: `TimingEvent.JOB_START_TIME`
    EventTimer jobSuccessTimer = timerFactory.createJobTimer();
    Optional<GenerateWorkUnitsResult> optGenerateWorkUnitResult = Optional.empty();
    WUProcessingSpec wuSpec = createProcessingSpec(jobProps, eventSubmitterContext);
    boolean isSuccessful = false;
    try (Closer closer = Closer.create()) {
      final GenerateWorkUnits genWUsActivityStub = Workflow.newActivityStub(GenerateWorkUnits.class,
          ActivityType.GENERATE_WORKUNITS.buildActivityOptions(temporalJobProps, true));
      GenerateWorkUnitsResult generateWorkUnitResult = genWUsActivityStub.generateWorkUnits(jobProps, eventSubmitterContext);
      optGenerateWorkUnitResult = Optional.of(generateWorkUnitResult);
      WorkUnitsSizeSummary wuSizeSummary = generateWorkUnitResult.getWorkUnitsSizeSummary();
      int numWUsGenerated = safelyCastNumConstituentWorkUnitsOrThrow(wuSizeSummary);
      int numWUsCommitted = 0;
      CommitStats commitStats = CommitStats.createEmpty();
      if (numWUsGenerated > 0) {
        TimeBudget timeBudget = calcWUProcTimeBudget(jobSuccessTimer.getStartTime(), wuSizeSummary, jobProps);
        final RecommendScalingForWorkUnits recommendScalingStub = Workflow.newActivityStub(RecommendScalingForWorkUnits.class,
            ActivityType.RECOMMEND_SCALING.buildActivityOptions(temporalJobProps, false));
        List<ScalingDirective> scalingDirectives =
            recommendScalingStub.recommendScaling(wuSizeSummary, generateWorkUnitResult.getSourceClass(), timeBudget, jobProps);
        log.info("Recommended scaling to process WUs within {}: {}", timeBudget, scalingDirectives);
        try {
          ScalingDirectivesRecipient recipient = createScalingDirectivesRecipient(jobProps, closer);
          List<ScalingDirective> adjustedScalingDirectives = adjustRecommendedScaling(jobProps, scalingDirectives);
          log.info("Submitting (adjusted) scaling directives: {}", adjustedScalingDirectives);
          recipient.receive(adjustedScalingDirectives);
          // TODO: when eliminating the "GenWUs Worker", pause/block until scaling is complete
        } catch (IOException e) {
          // TODO: decide whether this should be a hard failure; for now, "gracefully degrade" by continuing processing
          log.error("Failed to send re-scaling directive", e);
        }

        ProcessWorkUnitsWorkflow processWUsWorkflow = createProcessWorkUnitsWorkflow(jobProps);
        commitStats = processWUsWorkflow.process(wuSpec, temporalJobProps);
        numWUsCommitted = commitStats.getNumCommittedWorkUnits();
      }
      jobSuccessTimer.stop();
      isSuccessful = true;
      return new ExecGobblinStats(numWUsGenerated, numWUsCommitted, jobProps.getProperty(Help.USER_TO_PROXY_KEY),
          commitStats.getDatasetStats());
    } catch (Exception e) {
      // Emit a failed GobblinTrackingEvent to record job failures
      timerFactory.create(TimingEvent.LauncherTimings.JOB_FAILED).submit(); // update GaaS: `ExecutionStatus.FAILED`; `TimingEvent.JOB_END_TIME`
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed Gobblin job %s", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)),
          e.getClass().getName(), e);
    } finally {
      // TODO: Cleanup WorkUnit/Taskstate Directory for jobs cancelled mid flight
      try {
        log.info("Cleaning up work dirs for job {}", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY));
        if (optGenerateWorkUnitResult.isPresent()) {
          cleanupWorkDirs(wuSpec, eventSubmitterContext, optGenerateWorkUnitResult.get().getWorkDirPathsToDelete());
        } else {
          log.warn("Skipping cleanup of work dirs for job due to no output from GenerateWorkUnits");
        }
      } catch (IOException e) {
        // Only fail the job with a new failure if the job was successful, otherwise keep the original error
        if (isSuccessful) {
          throw ApplicationFailure.newNonRetryableFailureWithCause(
              String.format("Failed cleaning Gobblin job %s", jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY)),
              e.getClass().getName(), e);
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

  protected TimeBudget calcWUProcTimeBudget(Instant jobStartTime, WorkUnitsSizeSummary wuSizeSummary, Properties jobProps) {
    // TODO: make fully configurable!  for now, cap Work Discovery at 45 mins and set aside 10 mins for the `CommitStepWorkflow`
    long maxGenWUsMins = 45;
    long commitStepMins = 15;
    long totalTargetTimeMins = TimeUnit.MINUTES.toMinutes(PropertiesUtils.getPropAsLong(jobProps,
        ConfigurationKeys.JOB_TARGET_COMPLETION_DURATION_IN_MINUTES_KEY,
        ConfigurationKeys.DEFAULT_JOB_TARGET_COMPLETION_DURATION_IN_MINUTES));
    double permittedOveragePercentage = .2;

    // since actual generate WU duration can vary significantly across jobs, removing that from computation to enable deterministic duration for WU processing
    // Duration genWUsDuration = Duration.between(jobStartTime, TemporalEventTimer.WithinWorkflowFactory.getCurrentInstant());
    long remainingMins = totalTargetTimeMins - maxGenWUsMins - commitStepMins;
    return TimeBudget.withOveragePercentage(remainingMins, permittedOveragePercentage);
  }

  protected List<ScalingDirective> adjustRecommendedScaling(Properties jobProps, List<ScalingDirective> recommendedScalingDirectives) {
    // TODO: make any adjustments - e.g. decide whether to shutdown the (often oversize) `GenerateWorkUnits` worker or alternatively to deduct one to count it
    if (recommendedScalingDirectives.size() == 0) {
      return recommendedScalingDirectives;
    }
    // TODO: be more robust and code more defensively, rather than presuming the impl of `RecommendScalingForWorkUnitsLinearHeuristicImpl`
    ArrayList<ScalingDirective> adjustedScaling = new ArrayList<>(recommendedScalingDirectives);
    ScalingDirective firstDirective = adjustedScaling.get(0);
    // deduct one for (already existing) `GenerateWorkUnits` worker (we presume its "baseline" `WorkerProfile` similar enough to substitute for this new one)
    int initialContainerCount = Integer.parseInt(jobProps.getProperty(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY, "1"));
    adjustedScaling.set(0, firstDirective.updateSetPoint(firstDirective.getSetPoint() - initialContainerCount));
    // CAUTION: filter out set point zero, which (depending upon `.getProfileName()`) *could* down-scale away our only current worker
    // TODO: consider whether to allow either a) "pre-defining" a profile w/ set point zero, available for later use OR b) down-scaling to zero to pause worker
    return adjustedScaling.stream().filter(sd -> sd.getSetPoint() > 0).collect(Collectors.toList());
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

  protected ScalingDirectivesRecipient createScalingDirectivesRecipient(Properties jobProps, Closer closer) throws IOException {
    JobState jobState = new JobState(jobProps);
    FileSystem fs = closer.register(JobStateUtils.openFileSystem(jobState));
    Config jobConfig = ConfigUtils.propertiesToConfig(jobProps);
    String appName = jobConfig.getString(GobblinYarnConfigurationKeys.APPLICATION_NAME_KEY);
    // *hopefully* `GobblinClusterConfigurationKeys.CLUSTER_EXACT_WORK_DIR` is among `job.Config`!  if so, everything Just Works, but if not...
    // there's not presently an easy way to obtain the yarn app ID (like `application_1734430124616_67239`), so we'd need to plumb one through,
    // almost certainly based on `org.apache.gobblin.temporal.cluster.GobblinTemporalTaskRunner.taskRunnerId`
    String applicationId = "__WARNING__NOT_A_REAL_APPLICATION_ID__";
    Path appWorkDir = GobblinClusterUtils.getAppWorkDirPathFromConfig(jobConfig, fs, appName, applicationId);
    log.info("Using GobblinCluster work dir: {}", appWorkDir);

    Path directivesDirPath = JobStateUtils.getDynamicScalingPath(appWorkDir);
    return new FsScalingDirectivesRecipient(fs, directivesDirPath);
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

    final DeleteWorkDirsActivity deleteWorkDirsActivityStub = Workflow.newActivityStub(
        DeleteWorkDirsActivity.class,
        ActivityType.DELETE_WORK_DIRS.buildActivityOptions(jobState.getProperties(), false)
    );

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

  /**
   * Historical practice counted {@link org.apache.gobblin.source.workunit.WorkUnit}s with {@link int} (see e.g. {@link JobState#getTaskCount()}).
   * Updated counting now uses {@link long}, although much code still presumes {@link int}.  While we don't presently anticipate jobs exceeding 2 billion
   * `WorkUnit`s, if it were ever to happen, this method will fail-fast to flag the need to address.
   * @throws {@link IllegalArgumentException} if the count exceeds {@link Integer#MAX_VALUE}
   */
  protected static int safelyCastNumConstituentWorkUnitsOrThrow(WorkUnitsSizeSummary wuSizeSummary) {
    long n = wuSizeSummary.getConstituentWorkUnitsCount();
    if (n > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Too many constituent WorkUnits (" + n + ") - exceeds `Integer.MAX_VALUE`!");
    }
    return (int) n;
  }
}
