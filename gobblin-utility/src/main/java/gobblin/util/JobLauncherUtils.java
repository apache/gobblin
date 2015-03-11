/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.google.common.base.Strings;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * Utility class for the job scheduler and job launchers.
 *
 * @author ynli
 */
public class JobLauncherUtils {

  /**
   * Create a new job ID.
   *
   * @param jobName job name
   * @return new job ID
   */
  public static String newJobId(String jobName) {
    // Job ID in the form of job_<job_id_suffix>
    // <job_id_suffix> is in the form of <job_name>_<current_timestamp>
    String jobIdSuffix = String.format("%s_%d", jobName, System.currentTimeMillis());
    return "job_" + jobIdSuffix;
  }

  /**
   * Create a new task ID for the job with the given job ID.
   *
   * @param jobId job ID
   * @param sequence task sequence number
   * @return new task ID
   */
  public static String newTaskId(String jobId, int sequence) {
    return String.format("task_%s_%d", jobId.substring(jobId.indexOf('_') + 1), sequence);
  }

  /**
   * Create an ID for a new multi-task (corresponding to a {@link gobblin.source.workunit.MultiWorkUnit})
   * for the job with the given job ID.
   *
   * @param jobId job ID
   * @param sequence multi-task sequence number
   * @return new multi-task ID
   */
  public static String newMultiTaskId(String jobId, int sequence) {
    return String.format("multitask_%s_%d", jobId.substring(jobId.indexOf('_') + 1), sequence);
  }

  /**
   * Cleanup staging data of a Gobblin task.
   *
   * @param taskState task state
   */
  public static void cleanStagingData(State taskState, Logger logger) throws IOException {
    int branches = taskState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    for (int i = 0; i < branches; i++) {
      String writerFsUri = taskState
          .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, branches, i),
              ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(writerFsUri), new Configuration());

      String writerFilePath = taskState
          .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, branches, i));
      if (Strings.isNullOrEmpty(writerFilePath)) {
        // The job may be cancelled before the task starts, so this may not be set.
        continue;
      }

      String stagingDirKey =
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_STAGING_DIR, branches, i);
      if (taskState.contains(stagingDirKey)) {
        Path stagingPath = new Path(taskState.getProp(stagingDirKey), writerFilePath);
        if (fs.exists(stagingPath)) {
          logger.info("Cleaning up staging directory " + stagingPath.toUri().getPath());
          fs.delete(stagingPath, true);
        }
      }

      String outputDirKey =
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_OUTPUT_DIR, branches, i);
      if (taskState.contains(outputDirKey)) {
        Path outputPath = new Path(taskState.getProp(outputDirKey), writerFilePath);
        if (fs.exists(outputPath)) {
          logger.info("Cleaning up output directory " + outputPath.toUri().getPath());
          fs.delete(outputPath, true);
        }
      }
    }
  }
}
