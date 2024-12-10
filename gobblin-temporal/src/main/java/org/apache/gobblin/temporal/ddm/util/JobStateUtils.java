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

package org.apache.gobblin.temporal.ddm.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.io.Closer;
import com.typesafe.config.ConfigFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.AbstractJobLauncher;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.SourceDecorator;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.ddm.work.EagerFsDirBackedWorkUnitClaimCheckWorkload;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.ParallelRunner;


/**
 * Utilities for applying {@link JobState} info to various ends:
 * - opening the {@link FileSystem}
 * - creating the {@link Source}
 * - creating a {@link SharedResourcesBroker}
 * - opening the {@link StateStore<TaskState>}
 * - writing serialized {@link WorkUnit}s to the {@link FileSystem}
 * - writing serialized {@link JobState} to the {@link FileSystem}
 */
@Slf4j
public class JobStateUtils {
  public static final String INPUT_DIR_NAME = "input"; // following MRJobLauncher.INPUT_DIR_NAME
  public static final String OUTPUT_DIR_NAME = "output"; // following MRJobLauncher.OUTPUT_DIR_NAME
  public static final boolean DEFAULT_WRITE_PREVIOUS_WORKUNIT_STATES = true;

  // reuse same handle among activities executed by the same worker
  private static final transient Cache<Path, StateStore<TaskState>> taskStateStoreByPath = CacheBuilder.newBuilder().build();

  private JobStateUtils() {}

  /** @return the {@link FileSystem} indicated by {@link ConfigurationKeys#FS_URI_KEY} */
  public static FileSystem openFileSystem(JobState jobState) throws IOException {
    return Help.loadFileSystemForUriForce(getFileSystemUri(jobState), jobState);
  }

  /** @return the FQ class name, presumed configured as {@link ConfigurationKeys#SOURCE_CLASS_KEY} */
  public static String getSourceClassName(JobState jobState) {
    return jobState.getProp(ConfigurationKeys.SOURCE_CLASS_KEY);
  }

  /** @return a new instance of {@link Source}, identified by {@link ConfigurationKeys#SOURCE_CLASS_KEY} */
  public static Source<?, ?> createSource(JobState jobState) throws ReflectiveOperationException {
    Class<?> sourceClass = Class.forName(getSourceClassName(jobState));
    log.info("Creating source: '{}'", sourceClass.getName());
    Source<?, ?> source = new SourceDecorator<>(
        Source.class.cast(sourceClass.newInstance()),
        jobState.getJobId(), log);
    return source;
  }

  public static StateStore<TaskState> openTaskStateStore(JobState jobState, FileSystem fs) {
    try {
      Path taskStateStorePath = JobStateUtils.getTaskStateStorePath(jobState, fs);
      return taskStateStoreByPath.get(taskStateStorePath, () ->
          openTaskStateStoreUncached(jobState, fs)
      );
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  public static StateStore<TaskState> openTaskStateStoreUncached(JobState jobState, FileSystem fs) {
    Path taskStateStorePath = JobStateUtils.getTaskStateStorePath(jobState, fs);
    log.info("opening FS task state store at path '{}'", taskStateStorePath);
    return new FsStateStore<>(fs, taskStateStorePath.toUri().getPath(), TaskState.class);
  }

  /** @return the {@link URI} indicated by {@link ConfigurationKeys#FS_URI_KEY} */
  public static URI getFileSystemUri(JobState jobState) {
    return URI.create(jobState.getProp(ConfigurationKeys.FS_URI_KEY, ConfigurationKeys.LOCAL_FS_URI));
  }

  /**
   * ATTENTION: derives path according to {@link org.apache.gobblin.runtime.mapreduce.MRJobLauncher} conventions, using same
   * {@link ConfigurationKeys#MR_JOB_ROOT_DIR_KEY}
   * @return "base" dir root {@link Path} for work dir (parent of inputs, output task states, etc.)
   */
  public static Path getWorkDirRoot(JobState jobState) {
    return new Path(
        new Path(jobState.getProp(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY), jobState.getJobName()),
        jobState.getJobId());
  }

  /**
   * ATTENTION: derives path according to {@link org.apache.gobblin.runtime.mapreduce.MRJobLauncher} conventions, using same
   * {@link ConfigurationKeys#MR_JOB_ROOT_DIR_KEY}
   * @return {@link Path} where "input" {@link WorkUnit}s should reside
   */
  public static Path getWorkUnitsPath(JobState jobState) {
    return getWorkUnitsPath(getWorkDirRoot(jobState));
  }

  /**
   * @return {@link Path} where "input" {@link WorkUnit}s should reside
   */
  public static Path getWorkUnitsPath(Path workDirRoot) {
    return new Path(workDirRoot, INPUT_DIR_NAME);
  }

  /**
   * ATTENTION: derives path according to {@link org.apache.gobblin.runtime.mapreduce.MRJobLauncher} conventions, using same
   * {@link ConfigurationKeys#MR_JOB_ROOT_DIR_KEY}
   * @return {@link Path} to {@link FsStateStore<TaskState>} backing dir
   */
  public static Path getTaskStateStorePath(JobState jobState, FileSystem fs) {
    Path jobOutputPath = new Path(getWorkDirRoot(jobState), OUTPUT_DIR_NAME);
    return fs.makeQualified(jobOutputPath);
  }

  /**
   * write serialized {@link WorkUnit}s in parallel into files named to tunnel {@link org.apache.gobblin.util.WorkUnitSizeInfo}.
   * {@link EagerFsDirBackedWorkUnitClaimCheckWorkload} (and possibly others) may later recover such size info.
   */
  public static void writeWorkUnits(List<WorkUnit> workUnits, Path workDirRootPath, JobState jobState, FileSystem fs)
      throws IOException {
    String jobId = jobState.getJobId();
    Path targetDirPath = getWorkUnitsPath(workDirRootPath);

    int numThreads = ParallelRunner.getNumThreadsConfig(jobState.getProperties());
    Closer closer = Closer.create(); // (NOTE: try-with-resources syntax wouldn't allow `catch { closer.rethrow(t) }`)
    try {
      ParallelRunner parallelRunner = closer.register(new ParallelRunner(numThreads, fs));

      JobLauncherUtils.WorkUnitPathCalculator pathCalculator = new JobLauncherUtils.WorkUnitPathCalculator();
      int i = 0;
      for (WorkUnit workUnit : workUnits) {
        // tunnel each WU's size info via its filename, for `EagerFsDirBackedWorkUnitClaimCheckWorkload#extractTunneledWorkUnitSizeInfo`
        Path workUnitFile = pathCalculator.calcNextPathWithTunneledSizeInfo(workUnit, jobId, targetDirPath);
        if (i++ == 0) {
          log.info("Writing work unit file [first of {}]: '{}'", workUnits.size(), workUnitFile);
        }
        parallelRunner.serializeToFile(workUnit, workUnitFile);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }
  }

  /** write serialized `jobState` beneath `workDirRootPath` of `fs`, per {@link JobStateUtils#DEFAULT_WRITE_PREVIOUS_WORKUNIT_STATES} */
  public static void writeJobState(JobState jobState, Path workDirRootPath, FileSystem fs) throws IOException {
    writeJobState(jobState, workDirRootPath, fs, DEFAULT_WRITE_PREVIOUS_WORKUNIT_STATES);
  }

  /** write serialized `jobState` beneath `workDirRootPath` of `fs`, per whether to `writePreviousWorkUnitStates` */
  public static void writeJobState(JobState jobState, Path workDirRootPath, FileSystem fs, boolean writePreviousWorkUnitStates) throws IOException {
    Path targetPath = new Path(workDirRootPath, AbstractJobLauncher.JOB_STATE_FILE_NAME);
    try (DataOutputStream dataOutputStream = new DataOutputStream(fs.create(targetPath))) {
      log.info("Writing serialized jobState to '{}'", targetPath);
      jobState.write(dataOutputStream, false, writePreviousWorkUnitStates);
      log.info("Finished writing jobState to '{}'", targetPath);
    }
  }

  public static SharedResourcesBroker<GobblinScopeTypes> getSharedResourcesBroker(JobState jobState) {
    SharedResourcesBroker<GobblinScopeTypes> globalBroker =
        SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
            ConfigFactory.parseProperties(jobState.getProperties()),
            GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    return globalBroker.newSubscopedBuilder(new JobScopeInstance(jobState.getJobName(), jobState.getJobId())).build();
  }
}
