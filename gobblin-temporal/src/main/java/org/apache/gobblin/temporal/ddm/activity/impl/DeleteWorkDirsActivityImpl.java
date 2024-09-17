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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.temporal.failure.ApplicationFailure;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.ddm.activity.DeleteWorkDirsActivity;
import org.apache.gobblin.temporal.ddm.work.DirDeletionResult;
import org.apache.gobblin.temporal.ddm.work.WUProcessingSpec;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.workflows.metrics.EventSubmitterContext;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.WriterUtils;


@Slf4j
public class DeleteWorkDirsActivityImpl implements DeleteWorkDirsActivity {
  static String UNDEFINED_JOB_NAME = "<job_name_stub>";

  @Override
  public DirDeletionResult delete(WUProcessingSpec workSpec, EventSubmitterContext eventSubmitterContext, Set<String> workDirPaths) {
    // Ensure that non HDFS writers exit early as they rely on a different cleanup process, can consider consolidation in the future
    // through an abstracted cleanup method implemented at a writer level
    if (workDirPaths.isEmpty()) {
      return DirDeletionResult.createEmpty();
    }
    //TODO: Emit timers to measure length of cleanup step
    Optional<String> optJobName = Optional.empty();
    try {
      FileSystem fs = Help.loadFileSystem(workSpec);
      JobState jobState = Help.loadJobState(workSpec, fs);
      optJobName = Optional.ofNullable(jobState.getJobName());

      Map<String, Boolean> attemptedCleanedDirectories = jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_PER_TASK, ConfigurationKeys.DEFAULT_CLEANUP_STAGING_DATA_PER_TASK) ?
          cleanupStagingDataPerTask(jobState, workDirPaths) : cleanupStagingDataForEntireJob(jobState, workDirPaths);

      return new DirDeletionResult(attemptedCleanedDirectories);
    } catch (Exception e) {
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed to cleanup temporary folders for job %s", optJobName.orElse(UNDEFINED_JOB_NAME)),
          IOException.class.toString(),
          new IOException(e)
      );
    }
  }

  //TODO: Support task level deletes if necessary, currently it is deemed redundant due to collecting temp dirs during generate work unit step
  private static Map<String, Boolean> cleanupStagingDataPerTask(JobState jobState, Set<String> workDirPaths) throws IOException {
    throw new IOException("Clean up staging data by task is not supported. Please set " + ConfigurationKeys.CLEANUP_STAGING_DATA_PER_TASK + " to false.");
  }

  private static Map<String, Boolean> cleanupStagingDataForEntireJob(JobState state, Set<String> workDirPaths) throws IOException {

    String writerFsUri = state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = JobLauncherUtils.getFsWithProxy(state, writerFsUri, WriterUtils.getFsConfiguration(state));
    Map<String, Boolean> attemptedCleanedDirectories = new HashMap<>();

    for (String resource : workDirPaths) {
      Path pathToClean = new Path(resource);
      log.info("Deleting resource directory " + pathToClean);
      try {
        HadoopUtils.deletePath(fs, pathToClean, true);
        attemptedCleanedDirectories.put(resource, true);
      } catch (IOException e) {
        boolean doesExist = fs.exists(pathToClean);
        // Only record failure to clean if the directory still exists, if it does not then we can assume it was already cleaned by another process
        if (doesExist) {
          log.error("Failed to delete resource directory " + pathToClean, e);
          attemptedCleanedDirectories.put(resource, false);
        }
      }
    }
    return attemptedCleanedDirectories;
  }
}
