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
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Lists;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.runtime.AbstractTaskStateTracker;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskCreationException;
import org.apache.gobblin.runtime.TaskExecutor;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.NoopAutomaticTroubleshooter;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.temporal.ddm.activity.ProcessWorkUnit;
import org.apache.gobblin.temporal.ddm.util.JobStateUtils;
import org.apache.gobblin.temporal.ddm.work.assistance.Help;
import org.apache.gobblin.temporal.ddm.work.WorkUnitClaimCheck;
import org.apache.gobblin.util.JobLauncherUtils;


@Slf4j
public class ProcessWorkUnitImpl implements ProcessWorkUnit {
  // TODO: replace w/ JobLauncherUtils (once committed)!!!
  private static final String MULTI_WORK_UNIT_FILE_EXTENSION = ".mwu";
  private static final int LOG_EXTENDED_PROPS_EVERY_WORK_UNITS_STRIDE = 100;

  @Override
  public int processWorkUnit(WorkUnitClaimCheck wu) {
    try (FileSystem fs = Help.loadFileSystemForce(wu)) {
      List<WorkUnit> workUnits = loadFlattenedWorkUnits(wu, fs);
      log.info("WU [{}] - loaded {} workUnits", wu.getCorrelator(), workUnits.size());
      JobState jobState = Help.loadJobState(wu, fs);
      return execute(workUnits, wu, jobState, fs);
    } catch (IOException | InterruptedException e) {
      /* for testing:
      return countSumProperties(workUnits, wu);
    } catch (IOException e) {
       */
      throw new RuntimeException(e);
    }
  }

  protected List<WorkUnit> loadFlattenedWorkUnits(WorkUnitClaimCheck wu, FileSystem fs) throws IOException {
    Path wuPath = new Path(wu.getWorkUnitPath());
    WorkUnit workUnit = (wuPath.toString().endsWith(MULTI_WORK_UNIT_FILE_EXTENSION)
        ? MultiWorkUnit.createEmpty()
        : WorkUnit.createEmpty());
    Help.deserializeStateWithRetries(fs, wuPath, workUnit, wu);

    return workUnit instanceof MultiWorkUnit
        ? JobLauncherUtils.flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits())
        : Lists.newArrayList(workUnit);
  }

  /**
   * NOTE: adapted from {@link org.apache.gobblin.runtime.mapreduce.MRJobLauncher}!!!
   * @return count of how many tasks executed (0 if execution ultimately failed, although we *believe* TaskState would have already been recorded beforehand)
   */
  protected int execute(List<WorkUnit> workUnits, WorkUnitClaimCheck wu, JobState jobState, FileSystem fs) throws IOException, InterruptedException {
    String containerId = "container-id-for-wu-" + wu.getCorrelator();
    StateStore<TaskState> taskStateStore = Help.openTaskStateStore(wu, fs);

    TaskStateTracker taskStateTracker = createEssentializedTaskStateTracker(wu);
    TaskExecutor taskExecutor = new TaskExecutor(new Properties());
    GobblinMultiTaskAttempt.CommitPolicy multiTaskAttemptCommitPolicy = GobblinMultiTaskAttempt.CommitPolicy.IMMEDIATE; // as no speculative exec

    SharedResourcesBroker<GobblinScopeTypes> resourcesBroker = JobStateUtils.getSharedResourcesBroker(jobState);
    AutomaticTroubleshooter troubleshooter = new NoopAutomaticTroubleshooter();
    // AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(wu.getStateConfig().getProperties()));
    troubleshooter.start();

    List<String> fileSourcePaths = workUnits.stream()
        .map(workUnit -> describeAsCopyableFile(workUnit, wu.getWorkUnitPath()))
        .collect(Collectors.toList());
    log.info("WU [{}] - submitting {} workUnits for copying files: {}", wu.getCorrelator(),
        workUnits.size(), fileSourcePaths);
    log.debug("WU [{}] - (first) workUnit: {}", wu.getCorrelator(), workUnits.get(0).toJsonString());

    try {
      GobblinMultiTaskAttempt taskAttempt = GobblinMultiTaskAttempt.runWorkUnits(
          jobState.getJobId(), containerId, jobState, workUnits,
          taskStateTracker, taskExecutor, taskStateStore, multiTaskAttemptCommitPolicy,
          resourcesBroker, troubleshooter.getIssueRepository(), createInterruptionPredicate(fs, jobState));
      return taskAttempt.getNumTasksCreated();
    } catch (TaskCreationException tce) { // derived type of `IOException` that ought not be caught!
      throw tce;
    } catch (IOException ioe) {
      // presume execution already occurred, with `TaskState` written to reflect outcome
      log.warn("WU [" + wu.getCorrelator() + "] - continuing on despite IOException:", ioe);
      return 0;
    }
  }

  /** Demonstration processing to debug WU loading and deserialization */
  protected int countSumProperties(List<WorkUnit> workUnits, WorkUnitClaimCheck wu) {
    int totalNumProps = workUnits.stream().mapToInt(workUnit -> workUnit.getPropertyNames().size()).sum();
    log.info("opened WU [{}] to find {} properties total at '{}'", wu.getCorrelator(), totalNumProps, wu.getWorkUnitPath());
    return totalNumProps;
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
        log.info("WU [{} = {}] - finished commit after {}ms with state {}{}", wu.getCorrelator(), task.getTaskId(),
            taskState.getTaskDuration(), taskState.getWorkingState(),
            taskState.getWorkingState().equals(WorkUnitState.WorkingState.SUCCESSFUL)
                ? (" to: " + taskState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR)) : "");
        log.debug("WU [{} = {}] - task state: {}", wu.getCorrelator(), task.getTaskId(),
            taskState.toJsonString(shouldUseExtendedLogging(wu)));
        getOptCopyableFile(taskState).ifPresent(copyableFile -> {
          log.info("WU [{} = {}] - completed copyableFile: {}", wu.getCorrelator(), task.getTaskId(),
              copyableFile.toJsonString(shouldUseExtendedLogging(wu)));
        });
      }
    };
  }

  protected String describeAsCopyableFile(WorkUnit workUnit, String workUnitPath) {
    return getOptFirstCopyableFile(Lists.newArrayList(workUnit), workUnitPath)
        .map(copyableFile -> copyableFile.getOrigin().getPath().toString())
        .orElse(
            "<<not a CopyableFile("
                + getOptCopyEntityClass(workUnit, workUnitPath)
                .map(Class::getSimpleName)
                .orElse("<<not a CopyEntity!>>")
                + "): '" + workUnitPath + "'"
        );
  }

  protected Optional<CopyableFile> getOptCopyableFile(TaskState taskState) {
    return getOptCopyableFile(taskState, "taskState '" + taskState.getTaskId() + "'");
  }

  protected Optional<CopyableFile> getOptFirstCopyableFile(List<WorkUnit> workUnits, String workUnitPath) {
    return Optional.of(workUnits).filter(wus -> wus.size() > 0).flatMap(wus ->
      getOptCopyableFile(wus.get(0), "workUnit '" + workUnitPath + "'")
    );
  }

  protected Optional<CopyableFile> getOptCopyableFile(State state, String logDesc) {
    return getOptCopyEntityClass(state, logDesc).flatMap(copyEntityClass -> {
      log.debug("(state) {} got (copyEntity) {}", state.getClass().getName(), copyEntityClass.getName());
      if (CopyableFile.class.isAssignableFrom(copyEntityClass)) {
        String serialization = state.getProp(CopySource.SERIALIZED_COPYABLE_FILE);
        if (serialization != null) {
          return Optional.of((CopyableFile) CopyEntity.deserialize(serialization));
        }
      }
      return Optional.empty();
    });
  }

  protected Optional<Class<?>> getOptCopyEntityClass(State state, String logDesc) {
    try {
      return Optional.of(CopySource.getCopyEntityClass(state));
    } catch (IOException ioe) {
      log.warn(logDesc + " - failed getting copy entity class:", ioe);
      return Optional.empty();
    }
  }

  protected Predicate<GobblinMultiTaskAttempt> createInterruptionPredicate(FileSystem fs, JobState jobState) {
    // TODO - decide whether to support... and if so, employ a useful path; otherwise, just evaluate predicate to always false
    Path interruptionPath = new Path("/not/a/real/path/that/should/ever/exist!");
    return createInterruptionPredicate(fs, interruptionPath);
  }

  protected Predicate<GobblinMultiTaskAttempt> createInterruptionPredicate(FileSystem fs, Path interruptionPath) {
    return  (gmta) -> {
      try {
        return fs.exists(interruptionPath);
      } catch (IOException ioe) {
        return false;
      }
    };
  }

  protected boolean shouldUseExtendedLogging(WorkUnitClaimCheck wu) {
    try {
      return Long.valueOf(wu.getCorrelator()) % LOG_EXTENDED_PROPS_EVERY_WORK_UNITS_STRIDE == 0;
    } catch (NumberFormatException nfe) {
      log.warn("unexpected, non-numeric correlator: '{}'", wu.getCorrelator());
      return false;
    }
  }
}
