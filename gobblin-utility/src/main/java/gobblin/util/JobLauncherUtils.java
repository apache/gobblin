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
   * @param state workunit state
   */
  public static void cleanStagingData(State state, Logger logger) throws IOException {
    int branches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    for (int i = 0; i < branches; i++) {
      String writerFsUri =
          state.getProp(
              ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, branches, i),
              ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(writerFsUri), new Configuration());

      Path stagingPath = WriterUtils.getWriterStagingDir(state, branches > 1 ? i : -1);
      if (fs.exists(stagingPath)) {
        logger.info("Cleaning up staging directory " + stagingPath.toUri().getPath());
        fs.delete(stagingPath, true);
      }

      Path outputPath = WriterUtils.getWriterOutputDir(state, branches > 1 ? i : -1);
      if (fs.exists(outputPath)) {
        logger.info("Cleaning up output directory " + outputPath.toUri().getPath());
        fs.delete(outputPath, true);
      }
    }
  }
}
