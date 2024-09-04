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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import io.temporal.failure.ApplicationFailure;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.commit.DeliverySemantics;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobException;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.temporal.ddm.activity.DeleteWorkDirsActivity;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.CleanupResult;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.WriterUtils;


@Slf4j
public class DeleteWorkDirsActivityImpl implements DeleteWorkDirsActivity {
  static String UNDEFINED_JOB_NAME = "<job_name_stub>";

  @Override
  public CleanupResult cleanup(WUProcessingSpec workSpec, EventSubmitterContext eventSubmitterContext, Set<String> resourcesToClean) {
    //TODO: Emit timers to measure length of cleanup step
    Optional<String> optJobName = Optional.empty();
    try {
      FileSystem fs = Help.loadFileSystem(workSpec);
      JobState jobState = Help.loadJobState(workSpec, fs);
      optJobName = Optional.ofNullable(jobState.getJobName());

      SharedResourcesBroker<GobblinScopeTypes> instanceBroker = JobStateUtils.getSharedResourcesBroker(jobState);
      JobContext jobContext = new JobContext(jobState.getProperties(), log, instanceBroker, null);
      if (PropertiesUtils.getPropAsBoolean(jobState.getProperties(), ConfigurationKeys.CLEANUP_STAGING_DATA_BY_INITIALIZER, "false")) {
        //Clean up will be done by initializer.
        return CleanupResult.createEmpty();
      }
      try {
        if (!canCleanStagingData(jobContext, jobState)) {
          log.error("Job " + jobState.getJobName() + " has unfinished commit sequences. Will not clean up staging data.");
          return CleanupResult.createEmpty();
        }
      } catch (IOException e) {
        throw new JobException("Failed to check unfinished commit sequences", e);
      }
      Map<String, Long> cleanupMetrics = jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_PER_TASK, ConfigurationKeys.DEFAULT_CLEANUP_STAGING_DATA_PER_TASK) ?
          cleanupStagingDataPerTask(jobState, workSpec) : cleanupStagingDataForEntireJob(jobState, resourcesToClean, workSpec);

      return new CleanupResult(cleanupMetrics);
    } catch (Exception e) {
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed to cleanup temporary folders for job %s", optJobName.orElse(UNDEFINED_JOB_NAME)),
          IOException.class.toString(),
          new IOException(e)
      );
    }
  }

  private boolean canCleanStagingData(JobContext jobContext, JobState jobState)
      throws IOException {
    return jobContext.getSemantics() != DeliverySemantics.EXACTLY_ONCE || !jobContext.getCommitSequenceStore()
        .get().exists(jobState.getJobName());
  }

  // Goes through all the task states and cleans them up individually, there is some overlap with the job level cleanup as the job level cleanup will
  // also clean up the task level staging data
  private static Map<String, Long> cleanupStagingDataPerTask(JobState jobState, WUProcessingSpec workSpec) throws IOException {
    Closer closer = Closer.create();
    Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
    FileSystem fs = Help.loadFileSystem(workSpec);
    int numThreads = jobState.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);
    List<TaskState> taskStateList = Help.loadTaskStates(workSpec, fs, jobState, numThreads);
    Map<String, Long> cleanupMetrics = Maps.newHashMap();
    try {
      for (TaskState taskState : taskStateList) {
        try {
          cleanupMetrics.putAll(cleanTaskStagingData(taskState, log, closer, parallelRunners));
        } catch (IOException e) {
          log.error(String.format("Failed to clean staging data for task %s: %s", taskState.getTaskId(), e), e);
        }
      }
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        log.error("Failed to clean staging data", e);
      }
    }
    return cleanupMetrics;
  }

  private static Map<String, Long> cleanupStagingDataForEntireJob(JobState state, Set<String> resourcesToClean, WUProcessingSpec workSpec) throws IOException {
    if (!state.contains(ConfigurationKeys.WRITER_STAGING_DIR) || !state.contains(ConfigurationKeys.WRITER_OUTPUT_DIR)) {
      return Maps.newHashMap();
    }
    String writerFsUri = state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = JobLauncherUtils.getFsWithProxy(state, writerFsUri, WriterUtils.getFsConfiguration(state));
    Map<String, Long> cleanupMetrics = Maps.newHashMap();
    String jobId = state.getJobId();

    for (String resource : resourcesToClean) {
      Path pathToClean = new Path(resource);
      Long filesCleaned = fs.getContentSummary(pathToClean).getLength();
      log.info("Cleaning up resource directory " + pathToClean);
      if (!isDirectoryJobLevelScoped(resource, jobId)) {
        HadoopUtils.deletePath(fs, pathToClean, true);
        cleanupMetrics.put(pathToClean.toString(), filesCleaned);
      } else {
        log.warn("Not deleting directory " + pathToClean.toUri().getPath() + " as it is not job level scoped");
      }
    }

    Set<Path> singleLevelParentDirs = resourcesToClean.stream().map(resource -> new Path(resource).getParent()).collect(
        Collectors.toSet());

    for (Path parentDir : singleLevelParentDirs) {
      if (fs.exists(parentDir) && fs.listStatus(parentDir).length == 0) {
        log.info("Deleting directory " + parentDir);
        if (!isDirectoryJobLevelScoped(parentDir.toString(), jobId)) {
          HadoopUtils.deletePath(fs, parentDir, true);
          cleanupMetrics.put(parentDir.toString(), 0L);
        } else {
          log.warn("Not deleting directory " + parentDir.toUri().getPath() + " as it is not job level scoped");
        }
      }
    }
    return cleanupMetrics;
  }

  public static Map<String, Long> cleanTaskStagingData(TaskState state, Logger log, Closer closer,
      Map<String, ParallelRunner> parallelRunners) throws IOException {
    int numBranches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    int parallelRunnerThreads =
        state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);

    Map<String, Long> cleanupMetrics = Maps.newHashMap();
    String jobId = state.getJobId();
    for (int branchId = 0; branchId < numBranches; branchId++) {
      String writerFsUri = state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
          ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = JobLauncherUtils.getFsWithProxy(state, writerFsUri, WriterUtils.getFsConfiguration(state));
      ParallelRunner parallelRunner = JobLauncherUtils.getParallelRunner(fs, closer, parallelRunnerThreads, parallelRunners);

      Path stagingPath = WriterUtils.getWriterStagingDir(state, numBranches, branchId);
      if (fs.exists(stagingPath)) {
        log.info("Cleaning up staging directory " + stagingPath.toUri().getPath());
        Long filesCleaned = fs.getContentSummary(stagingPath).getLength();
        if (!isDirectoryJobLevelScoped(stagingPath.toString(), jobId)) {
          parallelRunner.deletePath(stagingPath, true);
          if (!cleanupMetrics.containsKey(stagingPath.toString())) {
            cleanupMetrics.put(stagingPath.toString(), filesCleaned);
          }
        } else {
          log.warn("Not deleting directory " + stagingPath.toUri().getPath() + " as it is not job level scoped");
        }
      }

      Path outputPath = WriterUtils.getWriterOutputDir(state, numBranches, branchId);
      if (fs.exists(outputPath)) {
        log.info("Cleaning up output directory " + outputPath.toUri().getPath());
        Long filesCleaned = fs.getContentSummary(outputPath).getLength();
        if (!isDirectoryJobLevelScoped(stagingPath.toString(), jobId)) {
          parallelRunner.deletePath(outputPath, true);
          if (!cleanupMetrics.containsKey(outputPath.toString())) {
            cleanupMetrics.put(outputPath.toString(), filesCleaned);
          }
        } else {
          log.warn("Not deleting directory " + outputPath.toUri().getPath() + " as it is not job level scoped");
        }
      }
    }
    return cleanupMetrics;
  }

  /**
   * Ensures that the directory is safe to delete if it is contained within a job level folder
   * @return
   */
  private static boolean isDirectoryJobLevelScoped(String path, String jobId) {
    return path.contains(jobId);
  }
}
