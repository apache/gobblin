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

package gobblin.yarn;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nullable;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.StateStore;
import gobblin.runtime.AbstractJobLauncher;
import gobblin.runtime.TaskContext;
import gobblin.runtime.TaskExecutor;
import gobblin.runtime.TaskState;
import gobblin.runtime.TaskStateTracker;
import gobblin.source.workunit.WorkUnit;


/**
 * An implementation of Helix's {@link Task} that wraps and runs a Gobblin {@link gobblin.runtime.Task}.
 *
 * @author ynli
 */
public class GobblinHelixTask implements Task {

  private static final Logger LOGGER = LoggerFactory.getLogger(GobblinHelixTask.class);

  private final TaskExecutor taskExecutor;
  private final TaskStateTracker taskStateTracker;

  private final TaskConfig taskConfig;
  private final String jobId;
  private final String taskId;

  private final FileSystem fs;
  private final StateStore<TaskState> taskStateStore;

  private volatile Optional<Future<?>> futureOptional = Optional.absent();

  public GobblinHelixTask(TaskCallbackContext taskCallbackContext, TaskExecutor taskExecutor,
      TaskStateTracker taskStateTracker, FileSystem fs, Path appWorkDir) throws IOException {
    this.taskExecutor = taskExecutor;
    this.taskStateTracker = taskStateTracker;

    this.taskConfig = taskCallbackContext.getTaskConfig();
    this.jobId = this.taskConfig.getConfigMap().get(ConfigurationKeys.JOB_ID_KEY);
    this.taskId = this.taskConfig.getConfigMap().get(ConfigurationKeys.TASK_ID_KEY);

    this.fs = fs;
    Path taskStateOutputDir = new Path(appWorkDir, ConfigurationConstants.OUTPUT_TASK_STATE_DIR_NAME);
    this.taskStateStore = new FsStateStore<TaskState>(this.fs, taskStateOutputDir.toString(), TaskState.class);
  }

  @Override
  public TaskResult run() {
    LOGGER.info(String.format("Running WorkUnit %s of job %s", taskId, jobId));

    try {
      // Create a new task from the work unit and submit the task to run
      gobblin.runtime.Task task = buildTask();
      this.taskStateTracker.registerNewTask(task);
      LOGGER.info(String.format("Submitting WorkUnit %s to run", this.taskId));
      this.futureOptional = Optional.<Future<?>>of(this.taskExecutor.submit(task));
      this.futureOptional.get().get();

      // Persist the task state so it is later collected by the job launcher
      persistTaskState(task.getTaskState());

      WorkUnitState.WorkingState workingState = task.getTaskState().getWorkingState();
      LOGGER.info(String.format("WorkUnit %s completed with state %s", this.taskId, workingState));
      switch (workingState) {
        case SUCCESSFUL:
          return new TaskResult(TaskResult.Status.COMPLETED, "task id: " + this.taskId);
        case FAILED:
          return new TaskResult(TaskResult.Status.ERROR, "task id: " + this.taskId);
        case CANCELLED:
          return new TaskResult(TaskResult.Status.CANCELED, "task id: " + this.taskId);
        default:
          throw new IllegalStateException("Unexpected result WorkUnit state: " + workingState);
      }
    } catch (IOException ioe) {
      LOGGER.error("Failed to deserialize WorkUnit", ioe);
      throw new RuntimeException("Failed to run WorkUnit " + this.taskId, ioe);
    } catch (ExecutionException ee) {
      throw new RuntimeException("Failed to run WorkUnit " + this.taskId, ee);
    } catch (InterruptedException ie) {
      throw new RuntimeException("Failed to run WorkUnit " + this.taskId, ie);
    }
  }

  @Override
  public void cancel() {
    LOGGER.info(String.format("Cancelling WorkUnit %s of job %s", this.taskId, this.jobId));
    this.futureOptional.transform(new Function<Future<?>, Object>() {
      @Nullable
      @Override
      public Object apply(Future<?> input) {
        return input.cancel(true);
      }
    });
  }

  private gobblin.runtime.Task buildTask() throws IOException {
    WorkUnitState workUnitState = new WorkUnitState(deserializeWorkUnit(
        this.taskConfig.getConfigMap().get(ConfigurationConstants.WORK_UNIT_FILE_PATH)));
    workUnitState.setId(this.taskId);
    workUnitState.setProp(ConfigurationKeys.JOB_ID_KEY, this.jobId);
    workUnitState.setProp(ConfigurationKeys.TASK_ID_KEY, this.taskId);

    // Create a new task from the work unit and submit the task to run
    return new gobblin.runtime.Task(new TaskContext(workUnitState), this.taskStateTracker,
        this.taskExecutor, Optional.<CountDownLatch>absent());
  }

  private WorkUnit deserializeWorkUnit(String workUnitFile) throws IOException {
    WorkUnit workUnit = new WorkUnit();

    Closer closer = Closer.create();
    try {
      Path workUnitFilePath = new Path(workUnitFile);
      InputStream is = closer.register(this.fs.open(workUnitFilePath));
      DataInputStream dis = closer.register((new DataInputStream(is)));
      workUnit.readFields(dis);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    return workUnit;
  }

  private void persistTaskState(TaskState taskState) throws IOException {
    this.taskStateStore.put(this.jobId, this.taskId + AbstractJobLauncher.TASK_STATE_STORE_TABLE_SUFFIX, taskState);
  }
}
