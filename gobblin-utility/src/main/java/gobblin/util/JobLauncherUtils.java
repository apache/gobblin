/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;

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
  private static Map<String, FileSystem> ownerAndFs = Maps.newConcurrentMap();

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
   * Utility method that takes in a {@link List} of {@link WorkUnit}s, and flattens them. It builds up
   * the flattened list by checking each element of the given list, and seeing if it is an instance of
   * {@link MultiWorkUnit}. If it is then it calls itself on the {@link WorkUnit}s returned by
   * {@link MultiWorkUnit#getWorkUnits()}. If not, then it simply adds the {@link WorkUnit} to the
   * flattened list.
   *
   * @param workUnits is a {@link List} containing either {@link WorkUnit}s or {@link MultiWorkUnit}s
   * @return a {@link List} of flattened {@link WorkUnit}s
   */
  public static List<WorkUnit> flattenWorkUnits(List<WorkUnit> workUnits) {
    List<WorkUnit> flattenedWorkUnits = Lists.newArrayList();
    for (WorkUnit workUnit : workUnits) {
      if (workUnit instanceof MultiWorkUnit) {
        flattenedWorkUnits.addAll(flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits()));
      } else {
        flattenedWorkUnits.add(workUnit);
      }
    }
    return flattenedWorkUnits;
  }

  /**
   * Cleanup the staging data for a list of Gobblin tasks. This method calls the
   * {@link #cleanStagingData(State, Logger)} method.
   *
   * @param states a {@link List} of {@link State}s that need their staging data cleaned
   */
  public static void cleanStagingData(List<? extends State> states, Logger logger) throws IOException {
    for (State state : states) {
      JobLauncherUtils.cleanStagingData(state, logger);
    }
  }

  /**
   * Cleanup staging data of all tasks of a job.
   *
   * @param state job state
   */
  public static void cleanJobStagingData(State state, Logger logger) throws IOException {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.WRITER_STAGING_DIR),
        "Missing required property " + ConfigurationKeys.WRITER_STAGING_DIR);
    Preconditions.checkArgument(state.contains(ConfigurationKeys.WRITER_OUTPUT_DIR),
        "Missing required property " + ConfigurationKeys.WRITER_OUTPUT_DIR);

    String writerFsUri = state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = getFsWithProxy(state, writerFsUri);

    Path jobStagingPath = new Path(state.getProp(ConfigurationKeys.WRITER_STAGING_DIR));
    logger.info("Cleaning up staging directory " + jobStagingPath);
    HadoopUtils.deletePath(fs, jobStagingPath, true);

    if (fs.exists(jobStagingPath.getParent()) && fs.listStatus(jobStagingPath.getParent()).length == 0) {
      logger.info("Deleting directory " + jobStagingPath.getParent());
      HadoopUtils.deletePath(fs, jobStagingPath.getParent(), true);
    }

    Path jobOutputPath = new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));
    logger.info("Cleaning up output directory " + jobOutputPath);
    HadoopUtils.deletePath(fs, jobOutputPath, true);

    if (fs.exists(jobOutputPath.getParent()) && fs.listStatus(jobOutputPath.getParent()).length == 0) {
      logger.info("Deleting directory " + jobOutputPath.getParent());
      HadoopUtils.deletePath(fs, jobOutputPath.getParent(), true);
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
      String writerFsUri = state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
          ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = getFsWithProxy(state, writerFsUri);

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

  /**
   * Cleanup staging data of a Gobblin task using a {@link ParallelRunner}
   *
   * @param state workunit state
   * @param closer a closer that registers the given map of ParallelRunners. The caller is responsible
   * for closing the closer after the cleaning is done.
   * @param parallelRunners a map from FileSystem URI to ParallelRunner.
   * @throws IOException
   */
  public static void cleanStagingData(State state, Logger logger, Closer closer,
      Map<String, ParallelRunner> parallelRunners) throws IOException {
    int numBranches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    int parallelRunnerThreads =
        state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);

    for (int branchId = 0; branchId < numBranches; branchId++) {
      String writerFsUri = state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
          ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = getFsWithProxy(state, writerFsUri);
      ParallelRunner parallelRunner = getParallelRunner(fs, closer, parallelRunnerThreads, parallelRunners);

      Path stagingPath = WriterUtils.getWriterStagingDir(state, numBranches, branchId);

      if (fs.exists(stagingPath)) {
        logger.info("Cleaning up staging directory " + stagingPath.toUri().getPath());
        parallelRunner.deletePath(stagingPath, true);
      }

      Path outputPath = WriterUtils.getWriterOutputDir(state, numBranches, branchId);
      if (fs.exists(outputPath)) {
        logger.info("Cleaning up output directory " + outputPath.toUri().getPath());
        parallelRunner.deletePath(outputPath, true);
      }
    }
  }

  private static FileSystem getFsWithProxy(State state, String writerFsUri) throws IOException {
    if (!state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      return FileSystem.get(URI.create(writerFsUri), new Configuration());
    } else {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME)),
          "State does not contain a proper proxy user name");
      String owner = state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME);
      if (ownerAndFs.containsKey(owner)) {
        return ownerAndFs.get(owner);
      } else {
        try {
          FileSystem proxiedFs =
              new ProxiedFileSystemWrapper().getProxiedFileSystem(state, ProxiedFileSystemWrapper.AuthType.KEYTAB,
                  state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION), writerFsUri);
          ownerAndFs.put(owner, proxiedFs);
          return proxiedFs;
        } catch (InterruptedException e) {
          throw new IOException(e);
        } catch (URISyntaxException e) {
          throw new IOException(e);
        }
      }
    }
  }

  private static ParallelRunner getParallelRunner(FileSystem fs, Closer closer, int parallelRunnerThreads,
      Map<String, ParallelRunner> parallelRunners) {
    String uriAndHomeDir = new Path(new Path(fs.getUri()), fs.getHomeDirectory()).toString();
    if (!parallelRunners.containsKey(uriAndHomeDir)) {
      parallelRunners.put(uriAndHomeDir, closer.register(new ParallelRunner(parallelRunnerThreads, fs)));
    }
    return parallelRunners.get(uriAndHomeDir);
  }
}
