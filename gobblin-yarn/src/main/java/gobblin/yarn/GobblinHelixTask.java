/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.yarn;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.JobState;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskState;
import gobblin.runtime.TaskStateTracker;
import gobblin.runtime.util.JobMetrics;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.JobLauncherUtils;
import gobblin.util.SerializationUtils;


/**
 * An implementation of Helix's {@link org.apache.helix.task.Task} that wraps and runs one or more Gobblin
 * {@link gobblin.runtime.Task}s.
 *
 * <p>
 *   Upon startup, a {@link GobblinHelixTask} reads the property
 *   {@link GobblinYarnConfigurationKeys#WORK_UNIT_FILE_PATH} for the path of the file storing a serialized
 *   {@link WorkUnit} on the {@link FileSystem} of choice and de-serializes the {@link WorkUnit}. Depending on
 *   if the serialized {@link WorkUnit} is a {@link MultiWorkUnit}, it then creates one or more Gobblin
 *   {@link gobblin.runtime.Task}s to run the {@link WorkUnit}(s) (possibly wrapped in the {@link MultiWorkUnit})
 *   and waits for the Gobblin {@link gobblin.runtime.Task}(s) to finish. Upon completion of the Gobblin
 *   {@link gobblin.runtime.Task}(s), it persists the {@link TaskState} of each {@link gobblin.runtime.Task} to
 *   a file that will be collected by the {@link GobblinHelixJobLauncher} later upon completion of the job.
 * </p>
 *
 * @author Yinan Li
 */
public class GobblinHelixTask implements Task {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixTask.class);

  @SuppressWarnings({ "unused", "FieldCanBeLocal" })
  private final Optional<JobMetrics> jobMetrics;
  private final TaskExecutor taskExecutor;
  private final TaskStateTracker taskStateTracker;

  private final TaskConfig taskConfig;
  // An empty JobState instance that will be filled with values read from the serialized JobState
  private final JobState jobState = new JobState();
  private final String jobId;
  private final String participantId;

  private final FileSystem fs;
  private final StateStore<TaskState> taskStateStore;

  public GobblinHelixTask(TaskCallbackContext taskCallbackContext, Optional<ContainerMetrics> containerMetrics,
      TaskExecutor taskExecutor, TaskStateTracker taskStateTracker, FileSystem fs, Path appWorkDir) throws IOException {
    this.taskExecutor = taskExecutor;
    this.taskStateTracker = taskStateTracker;

    this.taskConfig = taskCallbackContext.getTaskConfig();
    this.jobId = this.taskConfig.getConfigMap().get(ConfigurationKeys.JOB_ID_KEY);
    this.participantId = taskCallbackContext.getManager().getInstanceName();

    this.fs = fs;
    Path taskStateOutputDir = new Path(appWorkDir, GobblinYarnConfigurationKeys.OUTPUT_TASK_STATE_DIR_NAME);
    this.taskStateStore = new FsStateStore<>(this.fs, taskStateOutputDir.toString(), TaskState.class);

    Path jobStateFilePath = new Path(appWorkDir, this.jobId + "." + AbstractJobLauncher.JOB_STATE_FILE_NAME);
    SerializationUtils.deserializeState(this.fs, jobStateFilePath, this.jobState);

    if (containerMetrics.isPresent()) {
      // This must be done after the jobState is deserialized from the jobStateFilePath
      // A reference to jobMetrics is required to ensure it is not evicted from the GobblinMetricsRegistry Cache
      this.jobMetrics = Optional.of(JobMetrics.get(this.jobState, containerMetrics.get().getMetricContext()));
    } else {
      this.jobMetrics = Optional.absent();
    }
  }

  @Override
  public TaskResult run() {
    try {
      Path workUnitFilePath =
          new Path(this.taskConfig.getConfigMap().get(GobblinYarnConfigurationKeys.WORK_UNIT_FILE_PATH));

      WorkUnit workUnit = workUnitFilePath.getName().endsWith(AbstractJobLauncher.MULTI_WORK_UNIT_FILE_EXTENSION)
          ? MultiWorkUnit.createEmpty() : WorkUnit.createEmpty();
      SerializationUtils.deserializeState(this.fs, workUnitFilePath, workUnit);

      // The list of individual WorkUnits (flattened) to run
      List<WorkUnit> workUnits = Lists.newArrayList();

      if (workUnit instanceof MultiWorkUnit) {
        // Flatten the MultiWorkUnit so the job configuration properties can be added to each individual WorkUnits
        List<WorkUnit> flattenedWorkUnits =
            JobLauncherUtils.flattenWorkUnits(((MultiWorkUnit) workUnit).getWorkUnits());
        workUnits.addAll(flattenedWorkUnits);
      } else {
        workUnits.add(workUnit);
      }

      AbstractJobLauncher.runWorkUnits(this.jobId, this.participantId, this.jobState, workUnits, this.taskStateTracker,
          this.taskExecutor, this.taskStateStore, LOGGER);
      return new TaskResult(TaskResult.Status.COMPLETED, String.format("completed tasks: %d", workUnits.size()));
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      return new TaskResult(TaskResult.Status.CANCELED, "");
    } catch (Throwable t) {
      LOGGER.error("GobblinHelixTask failed due to " + t.getMessage(), t);
      return new TaskResult(TaskResult.Status.ERROR, Throwables.getStackTraceAsString(t));
    }
  }

  @Override
  public void cancel() {
    // TODO: implement cancellation.
  }
}
