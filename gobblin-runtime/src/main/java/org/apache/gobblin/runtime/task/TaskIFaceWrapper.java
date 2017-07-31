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

package org.apache.gobblin.runtime.task;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.Task;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.runtime.TaskState;
import org.apache.gobblin.runtime.TaskStateTracker;
import org.apache.gobblin.runtime.fork.Fork;


/**
 * An extension of {@link Task} that wraps a generic {@link TaskIFace} for backwards compatibility.
 */
public class TaskIFaceWrapper extends Task {

  private final TaskIFace underlyingTask;
  private final TaskContext taskContext;
  private final String jobId;
  private final String taskId;
  private final CountDownLatch countDownLatch;
  private final TaskStateTracker taskStateTracker;
  private int retryCount = 0;

  public TaskIFaceWrapper(TaskIFace underlyingTask, TaskContext taskContext, CountDownLatch countDownLatch,
      TaskStateTracker taskStateTracker) {
    super();
    this.underlyingTask = underlyingTask;
    this.taskContext = taskContext;
    this.jobId = taskContext.getTaskState().getJobId();
    this.taskId = taskContext.getTaskState().getTaskId();
    this.countDownLatch = countDownLatch;
    this.taskStateTracker = taskStateTracker;
  }

  @Override
  public boolean awaitShutdown(long timeoutInMillis) throws InterruptedException {
    return this.underlyingTask.awaitShutdown(timeoutInMillis);
  }

  @Override
  public void shutdown() {
    this.underlyingTask.shutdown();
  }

  @Override
  public String getProgress() {
    return this.underlyingTask.getProgress();
  }

  @Override
  public void run() {
    this.underlyingTask.run();
    this.taskStateTracker.onTaskRunCompletion(this);
  }

  @Override
  public String getJobId() {
    return this.jobId;
  }

  @Override
  public String getTaskId() {
    return this.taskId;
  }

  @Override
  public TaskContext getTaskContext() {
    return this.taskContext;
  }

  @Override
  public TaskState getTaskState() {
    TaskState taskState = this.taskContext.getTaskState();
    taskState.setTaskId(getTaskId());
    taskState.setJobId(getJobId());
    taskState.setWorkingState(getWorkingState());
    taskState.addAll(getExecutionMetadata());
    taskState.addAll(getPersistentState());
    return taskState;
  }

  @Override
  public State getPersistentState() {
    return this.underlyingTask.getPersistentState();
  }

  @Override
  public State getExecutionMetadata() {
    return this.underlyingTask.getExecutionMetadata();
  }

  @Override
  public WorkUnitState.WorkingState getWorkingState() {
    return this.underlyingTask.getWorkingState();
  }

  @Override
  public List<Optional<Fork>> getForks() {
    return Lists.newArrayList();
  }

  @Override
  public void updateRecordMetrics() {
  }

  @Override
  public void updateByteMetrics() {
  }

  @Override
  public void incrementRetryCount() {
    this.retryCount++;
  }

  @Override
  public int getRetryCount() {
    return this.retryCount;
  }

  @Override
  public void markTaskCompletion() {
    if (this.countDownLatch != null) {
      this.countDownLatch.countDown();
    }
  }

  @Override
  public String toString() {
    return this.underlyingTask.toString();
  }

  @Override
  public void commit() {
    this.underlyingTask.commit();
    this.taskStateTracker.onTaskCommitCompletion(this);
  }

  @Override
  protected void submitTaskCommittedEvent() {
  }

  @Override
  public boolean isSpeculativeExecutionSafe() {
    return this.underlyingTask.isSpeculativeExecutionSafe();
  }
}
