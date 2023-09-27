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
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

import com.typesafe.config.ConfigFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.metastore.FsStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.AbstractTaskStateTracker;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.NoopAutomaticTroubleshooter;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.ddm.activity.ProcessWorkUnit;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.JobLauncherUtils;
import org.apache.gobblin.util.SerializationUtils;


@Slf4j
public class ProcessWorkUnitImpl implements ProcessWorkUnit {
  // TODO: replace w/ JobLauncherUtils (once committed)!!!
  private static final String MULTI_WORK_UNIT_FILE_EXTENSION = ".mwu";
  private static final String OUTPUT_DIR_NAME = "output";
  private static final String WRITER_OUTPUT_DIR_KEY = ConfigurationKeys.WRITER_OUTPUT_DIR;
  public static final int FS_CACHE_TTL_SECS = 60 * 60; // 5 * 60;

  private final State stateConfig;
  // cache `FileSystem`s to avoid re-opening what a recent prior execution already has
  private final transient LoadingCache<URI, FileSystem> fsByUri = CacheBuilder.newBuilder()
      //!!!TODO: choose appropriate caching eviction/extension so handles don't expire mid-execution!!!!
      .expireAfterWrite(FS_CACHE_TTL_SECS, TimeUnit.SECONDS)
      .removalListener((RemovalNotification<URI, org.apache.hadoop.fs.FileSystem> notification) -> {
        try {
          notification.getValue().close(); // prevent resource leak from cache eviction
        } catch (IOException ioe) {
          log.warn("trouble closing (cache-evicted) `hadoop.fs.FileSystem` at '" + notification.getKey() + "'", ioe);
          // otherwise swallow, since within a removal listener thread
        }
      })
      .build(new CacheLoader<URI, FileSystem>() {
    @Override
    public FileSystem load(URI fsUri) throws IOException {
      return ProcessWorkUnitImpl.this.loadFileSystemForUri(fsUri);
    }
  });

  public ProcessWorkUnitImpl(Optional<State> optStateConfig) {
    this.stateConfig = optStateConfig.orElseGet(State::new);
  }

  public ProcessWorkUnitImpl() {
    this(Optional.empty());
  }

  @Override
  public int processWorkUnit(WorkUnitClaimCheck wu) {
    try {
      FileSystem fs = this.loadFileSystem(wu);
      List<WorkUnit> workUnits = loadFlattenedWorkUnits(wu, fs);
      JobState jobState = loadJobState(wu, fs);
      String jobStateJson = jobState.toJsonString(true);
      log.info("WU [{}] - loaded jobState from '{}': {}", wu.getCorrelator(), wu.getJobStatePath(), jobStateJson);
      return execute(workUnits, wu, jobState, fs);
    } catch (IOException | InterruptedException e) {
      /* for testing:
      return countSumProperties(workUnits, wu);
    } catch (IOException e) {
       */
      throw new RuntimeException(e);
    }
  }

  protected final FileSystem loadFileSystem(WorkUnitClaimCheck wu) throws IOException {
    try {
      return fsByUri.get(wu.getNameNodeUri());
    } catch (ExecutionException ee) {
      throw new IOException(ee);
    }
  }

  protected List<WorkUnit> loadFlattenedWorkUnits(WorkUnitClaimCheck wu, FileSystem fs) throws IOException {
    Path wuPath = new Path(wu.getWorkUnitPath());
    WorkUnit workUnit = (wuPath.toString().endsWith(MULTI_WORK_UNIT_FILE_EXTENSION) ? MultiWorkUnit.createEmpty()
        : WorkUnit.createEmpty());
    // TODO/WARNING: `fs` runs the risk of expiring while executing!!! -
    SerializationUtils.deserializeState(fs, wuPath, workUnit);

    return workUnit instanceof MultiWorkUnit
        ? JobLauncherUtils.flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits())
        : Lists.newArrayList(workUnit);
  }

  protected JobState loadJobState(WorkUnitClaimCheck wu, FileSystem fs) throws IOException {
    Path jobStatePath = new Path(wu.getJobStatePath());
    JobState jobState = new JobState();
    // TODO/WARNING: `fs` runs the risk of expiring while executing!!! -
    SerializationUtils.deserializeState(fs, jobStatePath, jobState);
    return jobState;
  }

  /** CAUTION: uncached form! prefer caching of {@link #loadFileSystem(WorkUnitClaimCheck)} */
  protected FileSystem loadFileSystemForUri(URI fsUri) throws IOException {
    return HadoopUtils.getFileSystem(fsUri, stateConfig);
  }

  /**
   * NOTE: adapted from {@link org.apache.gobblin.runtime.mapreduce.MRJobLauncher}!!!
   * @return count of how many tasks submitted
   */
  protected int execute(List<WorkUnit> workUnits, WorkUnitClaimCheck wu, JobState jobState, FileSystem fs) throws IOException, InterruptedException {
    String containerId = "the-container-id"; // TODO: set this... if it actually matters!
    SharedResourcesBroker<GobblinScopeTypes> resourcesBroker = getSharedResourcesBroker(jobState);
    AutomaticTroubleshooter troubleshooter = new NoopAutomaticTroubleshooter();
    // AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(wu.getStateConfig().getProperties()));
    troubleshooter.start();

    Path taskStateStorePath = getTaskStateStorePath(jobState, fs, resourcesBroker, troubleshooter);
    log.info("WU [{}] - taskStateStore using path '{}'", wu.getCorrelator(), taskStateStorePath);
    StateStore<TaskState> taskStateStore = new FsStateStore<>(fs, taskStateStorePath.toUri().getPath(), TaskState.class);

    TaskStateTracker taskStateTracker = createEssentializedTaskStateTracker(wu);
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());
    GobblinMultiTaskAttempt.CommitPolicy multiTaskAttemptCommitPolicy = GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE; // as no speculative exec

    GobblinMultiTaskAttempt taskAttempt = GobblinMultiTaskAttempt.runWorkUnits(
        jobState.getJobId(), containerId, jobState,
        workUnits, taskStateTracker, taskExecutor, taskStateStore,
        multiTaskAttemptCommitPolicy, resourcesBroker, troubleshooter.getIssueRepository(),
        createInterruptionPredicate(fs, jobState));
    Map<String, Long> wuCountByOutputPath = workUnits.stream()
        .collect(Collectors.groupingBy(WorkUnit::getOutputFilePath, Collectors.counting()));
    log.info("WU [{}] - submitted {} workUnits; outputPathCounts = {}", wu.getCorrelator(),
        workUnits.size(), wuCountByOutputPath);
    // log.info("WU [{}] - (first) workUnit: {}", wu.getCorrelator(), workUnits.get(0).toJsonString());
    getOptFirstCopyableFile(workUnits, wu.getWorkUnitPath()).ifPresent(copyableFile -> {
        log.info("WU [{} = {}] - (first) copy entity: {}", wu.getCorrelator(), copyableFile.toJsonString());
    });
    return taskAttempt.getNumTasksCreated();
  }

  /** Demonstration processing to debug WU loading and deserialization */
  protected int countSumProperties(List<WorkUnit> workUnits, WorkUnitClaimCheck wu) {
    int totalNumProps = workUnits.stream().mapToInt(workUnit -> workUnit.getPropertyNames().size()).sum();
    log.info("opened WU [{}] to find {} properties total at '{}'", wu.getCorrelator(), totalNumProps, wu.getWorkUnitPath());
    return totalNumProps;
  }

  //!!!!!document dependence on MR_JOB_ROOT_DIR_KEY = "mr.job.root.dir";
  protected Path getTaskStateStorePath(JobState jobState, FileSystem fs, SharedResourcesBroker<GobblinScopeTypes> resourcesBroker,
      AutomaticTroubleshooter troubleshooter) {
    Properties jobProps = jobState.getProperties(); //???is this reasonable???
    JobContext jobContext = null;
    //!!!TODO - abstract this exception wrapping pattern!!!
    try {
      jobContext = new JobContext(jobProps, log, resourcesBroker, troubleshooter.getIssueRepository());
    } catch (Exception e) {
      new RuntimeException(e);
    }
    Path jobOutputPath = new Path(
        new Path(
            new Path(
                jobProps.getProperty(ConfigurationKeys.MR_JOB_ROOT_DIR_KEY),
                jobContext.getJobName()),
            jobContext.getJobId()),
        OUTPUT_DIR_NAME);
    return fs.makeQualified(jobOutputPath);
  }

  protected TaskStateTracker createEssentializedTaskStateTracker(WorkUnitClaimCheck wu) {
    return new AbstractTaskStateTracker(new Properties(), log) {
      @Override
      public void registerNewTask(Task task) {
        // TODO: shall we schedule metrics update based on config?
      }

      @Override
      public void onTaskRunCompletion(Task task) {
        task.markTaskCompletion();
      }

      @Override
      public void onTaskCommitCompletion(Task task) {
        TaskState taskState = task.getTaskState();
        // TODO: if metrics configured, report them now
        log.info("WU [{} = {}] - finished commit after {}ms with state {} to: {}", wu.getCorrelator(), task.getTaskId(),
            taskState.getTaskDuration(), taskState.getWorkingState(), taskState.getProp(WRITER_OUTPUT_DIR_KEY));
        log.info("WU [{} = {}] - task state: {}", wu.getCorrelator(), task.getTaskId(),
            taskState.toJsonString());
        getOptCopyableFile(taskState).ifPresent(copyableFile -> {
          log.info("WU [{} = {}] - copy entity: {}", wu.getCorrelator(), task.getTaskId(),
              copyableFile.toJsonString());
        });
      }
    };
  }

  protected Optional<CopyableFile> getOptCopyableFile(TaskState taskState) {
    return getOptCopyableFile(taskState, "taskState '" + taskState.getTaskId() + "'");
  }

  protected Optional<CopyableFile> getOptFirstCopyableFile(List<WorkUnit> workUnits, String workUnitPath) {
    if (workUnits != null && workUnits.size() > 0) {
      return getOptCopyableFile(workUnits.get(0), "workUnit '" + workUnitPath + "'");
    } else {
      return Optional.empty();
    }
  }

  protected Optional<CopyableFile> getOptCopyableFile(State state, String logDesc) {
    Class<?> copyEntityClass = null;
    try {
      copyEntityClass = CopySource.getCopyEntityClass(state);
    } catch (IOException ioe) {
      log.warn(logDesc + " - failed getting copy entity class:", ioe);
      return Optional.empty();
    }
    if (CopyableFile.class.isAssignableFrom(copyEntityClass)) {
      String serialization = state.getProp(CopySource.SERIALIZED_COPYABLE_FILE);
      if (serialization != null) {
        return Optional.of((CopyableFile) CopyEntity.deserialize(serialization));
      }
    }
    return Optional.empty();
  }

  protected SharedResourcesBroker<GobblinScopeTypes> getSharedResourcesBroker(JobState jobState) {
    SharedResourcesBroker<GobblinScopeTypes> globalBroker =
        SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
            ConfigFactory.parseProperties(jobState.getProperties()),
            GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    return globalBroker.newSubscopedBuilder(new JobScopeInstance(jobState.getJobName(), jobState.getJobId())).build();
  }

  Predicate<GobblinMultiTaskAttempt> createInterruptionPredicate(FileSystem fs, JobState jobState) {
    // TODO - decide whether to use, and if so, employ a useful path; otherwise, just evaluate predicate to always false
    Path interruptionPath = new Path("/not/a/real/path/that/should/ever/exist!");
    return createInterruptionPredicate(fs, interruptionPath);
  }

  Predicate<GobblinMultiTaskAttempt> createInterruptionPredicate(FileSystem fs, Path interruptionPath) {
    return  (gmta) -> {
      try {
        return fs.exists(interruptionPath);
      } catch (IOException ioe) {
        return false;
      }
    };
  }
}
