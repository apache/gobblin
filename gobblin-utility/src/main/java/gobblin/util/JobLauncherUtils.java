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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


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
   * Utility method that takes in a {@link List} of {@link State}s, and flattens them. It builds up the flattened
   * list by checking each element of the given list, and seeing if it is an instance of {@link MultiWorkUnit}. If it is
   * then it iterates through all the {@link WorkUnit}s returned by {@link MultiWorkUnit#getWorkUnits()} and adds them
   * to the flattened list. If not, then it simply adds the {@link WorkUnit} to the flattened list.
   * @param workUnits is a {@link List} containing either {@link State}s or {@link MultiWorkUnit}s.
   * @return a {@link List} of {@link State}s.
   */
  public static List<State> flattenWorkUnits(List<? extends State> states) {
    List<State> flattenedWorkUnits = Lists.newArrayList();
    for (State state : states) {
      if (state instanceof MultiWorkUnit) {
        for (State mulitWorkUnitChild : ((MultiWorkUnit) state).getWorkUnits()) {
          flattenedWorkUnits.add(mulitWorkUnitChild);
        }
      } else {
        flattenedWorkUnits.add(state);
      }
    }
    return flattenedWorkUnits;
  }

  /**
   * Cleanup the staging data for a list of Gobblin tasks. This method calls the {@link #cleanStagingData(State, Logger)}
   * method.
   *
   * @param states a {@list List} of {@link State}s that need their staging data cleaned.
   */
  public static void cleanStagingData(List<State> states, Logger logger) throws IOException {
    for (State state : states) {
      JobLauncherUtils.cleanStagingData(state, logger);
    }
  }

  /**
   * Cleanup staging data of a Gobblin task.
   *
   * @param state workunit state
   */
  public static void cleanStagingData(State state, Logger logger) throws IOException {
    int numBranches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    for (int branchId = 0; branchId < numBranches; branchId++) {
      String writerFsUri =
          state.getProp(
              ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
              ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(writerFsUri), new Configuration());

      Path stagingPath = WriterUtils.getWriterStagingDir(state, numBranches, branchId);
      if (fs.exists(stagingPath)) {
        logger.info("Cleaning up staging directory " + stagingPath.toUri().getPath());
        if (!fs.delete(stagingPath, true)) {
          throw new IOException("Clean up staging directory " + stagingPath.toUri().getPath() + " failed");
        }
      }

      Path outputPath = WriterUtils.getWriterOutputDir(state, numBranches, branchId);
      if (fs.exists(outputPath)) {
        logger.info("Cleaning up output directory " + outputPath.toUri().getPath());
        if (!fs.delete(outputPath, true)) {
          throw new IOException("Clean up output directory " + outputPath.toUri().getPath() + " failed");
        }
      }
    }
  }
}
