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

import com.google.common.collect.Maps;

import io.temporal.failure.ApplicationFailure;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.ddm.activity.DeleteWorkDirsActivity;
import org.apache.gobblin.temporal.ddm.work.CleanupResult;
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
  public CleanupResult delete(WUProcessingSpec workSpec, EventSubmitterContext eventSubmitterContext, Set<String> workDirPaths) {
    //TODO: Emit timers to measure length of cleanup step
    Optional<String> optJobName = Optional.empty();
    try {
      FileSystem fs = Help.loadFileSystem(workSpec);
      JobState jobState = Help.loadJobState(workSpec, fs);
      optJobName = Optional.ofNullable(jobState.getJobName());

      Map<String, Boolean> attemptedCleanedDirectories = jobState.getPropAsBoolean(ConfigurationKeys.CLEANUP_STAGING_DATA_PER_TASK, ConfigurationKeys.DEFAULT_CLEANUP_STAGING_DATA_PER_TASK) ?
          cleanupStagingDataPerTask(jobState, workDirPaths) : cleanupStagingDataForEntireJob(jobState, workDirPaths);

      return new CleanupResult(attemptedCleanedDirectories);
    } catch (Exception e) {
      throw ApplicationFailure.newNonRetryableFailureWithCause(
          String.format("Failed to cleanup temporary folders for job %s", optJobName.orElse(UNDEFINED_JOB_NAME)),
          IOException.class.toString(),
          new IOException(e)
      );
    }
  }

  private static Map<String, Boolean> cleanupStagingDataPerTask(JobState jobState, Set<String> resourcesToClean) throws IOException {
    log.warn("Clean up staging data by task is not supported, will clean up job level data instead");
    return cleanupStagingDataForEntireJob(jobState, resourcesToClean);
  }

  private static Map<String, Boolean> cleanupStagingDataForEntireJob(JobState state, Set<String> resourcesToClean) throws IOException {
    if (!state.contains(ConfigurationKeys.WRITER_STAGING_DIR) || !state.contains(ConfigurationKeys.WRITER_OUTPUT_DIR)) {
      return Maps.newHashMap();
    }
    String writerFsUri = state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = JobLauncherUtils.getFsWithProxy(state, writerFsUri, WriterUtils.getFsConfiguration(state));
    Map<String, Boolean> attemptedCleanedDirectories = new HashMap<>();

    for (String resource : resourcesToClean) {
      Path pathToClean = new Path(resource);
      log.info("Cleaning up resource directory " + pathToClean);
      try {
        HadoopUtils.deletePath(fs, pathToClean, true);
        attemptedCleanedDirectories.put(resource, true);
      } catch (IOException e) {
        log.error("Failed to clean up resource directory " + pathToClean, e);
        attemptedCleanedDirectories.put(resource, false);
      }
    }
    return attemptedCleanedDirectories;
  }
}
