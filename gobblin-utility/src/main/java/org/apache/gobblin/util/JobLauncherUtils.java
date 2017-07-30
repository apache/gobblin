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

package gobblin.util;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;


/**
 * Utility class for the job scheduler and job launchers.
 *
 * @author Yinan Li
 */
@Slf4j
public class JobLauncherUtils {

  // A cache for proxied FileSystems by owners
  private static Cache<String, FileSystem> fileSystemCacheByOwners = CacheBuilder.newBuilder().build();

  /**
   * Create a new job ID.
   *
   * @param jobName job name
   * @return new job ID
   */
  public static String newJobId(String jobName) {
    return Id.Job.create(jobName, System.currentTimeMillis()).toString();
  }

  /**
   * Create a new task ID for the job with the given job ID.
   *
   * @param jobId job ID
   * @param sequence task sequence number
   * @return new task ID
   */
  public static String newTaskId(String jobId, int sequence) {
    return Id.Task.create(Id.parse(jobId).get(Id.Parts.INSTANCE_NAME), sequence).toString();
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
    return Id.MultiTask.create(Id.parse(jobId).get(Id.Parts.INSTANCE_NAME), sequence).toString();
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
  public static List<WorkUnit> flattenWorkUnits(Collection<WorkUnit> workUnits) {
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
   * {@link #cleanTaskStagingData(State, Logger)} method.
   *
   * @param states a {@link List} of {@link State}s that need their staging data cleaned
   */
  public static void cleanStagingData(List<? extends State> states, Logger logger) throws IOException {
    for (State state : states) {
      JobLauncherUtils.cleanTaskStagingData(state, logger);
    }
  }

  /**
   * Cleanup staging data of all tasks of a job.
   *
   * @param state a {@link State} instance storing job configuration properties
   * @param logger a {@link Logger} used for logging
   */
  public static void cleanJobStagingData(State state, Logger logger) throws IOException {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.WRITER_STAGING_DIR),
        "Missing required property " + ConfigurationKeys.WRITER_STAGING_DIR);
    Preconditions.checkArgument(state.contains(ConfigurationKeys.WRITER_OUTPUT_DIR),
        "Missing required property " + ConfigurationKeys.WRITER_OUTPUT_DIR);

    String writerFsUri = state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    FileSystem fs = getFsWithProxy(state, writerFsUri, WriterUtils.getFsConfiguration(state));

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

    if (state.contains(ConfigurationKeys.ROW_LEVEL_ERR_FILE)) {
      if (state.getPropAsBoolean(ConfigurationKeys.CLEAN_ERR_DIR, ConfigurationKeys.DEFAULT_CLEAN_ERR_DIR)) {
        Path jobErrPath = new Path(state.getProp(ConfigurationKeys.ROW_LEVEL_ERR_FILE));
        log.info("Cleaning up err directory : " + jobErrPath);
        HadoopUtils.deleteIfExists(fs, jobErrPath, true);
      }
    }
  }

  /**
   * Cleanup staging data of a Gobblin task.
   *
   * @param state a {@link State} instance storing task configuration properties
   * @param logger a {@link Logger} used for logging
   */
  public static void cleanTaskStagingData(State state, Logger logger) throws IOException {
    int numBranches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    for (int branchId = 0; branchId < numBranches; branchId++) {
      String writerFsUri = state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
          ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = getFsWithProxy(state, writerFsUri, WriterUtils.getFsConfiguration(state));

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
   * Cleanup staging data of a Gobblin task using a {@link ParallelRunner}.
   *
   * @param state workunit state.
   * @param logger a {@link Logger} used for logging.
   * @param closer a closer that registers the given map of ParallelRunners. The caller is responsible
   * for closing the closer after the cleaning is done.
   * @param parallelRunners a map from FileSystem URI to ParallelRunner.
   * @throws IOException if it fails to cleanup the task staging data.
   */
  public static void cleanTaskStagingData(State state, Logger logger, Closer closer,
      Map<String, ParallelRunner> parallelRunners) throws IOException {
    int numBranches = state.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);

    int parallelRunnerThreads =
        state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);

    for (int branchId = 0; branchId < numBranches; branchId++) {
      String writerFsUri = state.getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, numBranches, branchId),
          ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = getFsWithProxy(state, writerFsUri, WriterUtils.getFsConfiguration(state));
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

  /**
   * @param state
   * @param fsUri
   * @return
   * @throws IOException
   */
  private static FileSystem getFsWithProxy(final State state, final String fsUri, final Configuration conf) throws IOException {
    if (!state.getPropAsBoolean(ConfigurationKeys.SHOULD_FS_PROXY_AS_USER,
        ConfigurationKeys.DEFAULT_SHOULD_FS_PROXY_AS_USER)) {
      return FileSystem.get(URI.create(fsUri), conf);
    }

    Preconditions.checkArgument(!Strings.isNullOrEmpty(state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME)),
        "State does not contain a proper proxy user name");
    String owner = state.getProp(ConfigurationKeys.FS_PROXY_AS_USER_NAME);

    try {
      return fileSystemCacheByOwners.get(owner, new Callable<FileSystem>() {

        @Override
        public FileSystem call()
            throws Exception {
          return new ProxiedFileSystemWrapper().getProxiedFileSystem(state, ProxiedFileSystemWrapper.AuthType.KEYTAB,
              state.getProp(ConfigurationKeys.SUPER_USER_KEY_TAB_LOCATION), fsUri, conf);
        }

      });
    } catch (ExecutionException ee) {
      throw new IOException(ee.getCause());
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
