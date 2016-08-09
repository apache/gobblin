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
package gobblin.runtime.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.runtime.JobState;
import gobblin.runtime.JobState.RunningState;

import lombok.Getter;

/**
 * TODO
 *
 */
public class JobExecutionState implements JobExecutionStatus {
  public static final String UKNOWN_STAGE = "unkown";

  @Getter private final JobExecution jobExecution;
  private final Optional<JobExecutionStateListener> listener;
  @Getter private JobState.RunningState runningState;
  /** Arbitrary execution stage, e.g. setup, workUnitGeneration, taskExecution, publishing */
  @Getter private String stage;
  @Getter final JobSpec jobSpec;
  Map<String, Object> executionMetadata;

  public JobExecutionState(JobSpec jobSpec, JobExecution jobExecution,
                           Optional<JobExecutionStateListener> listener) {
    this.jobExecution = jobExecution;
    this.runningState = null;
    this.stage = UKNOWN_STAGE;
    this.jobSpec = jobSpec;
    this.listener = listener;
    // TODO default implementation
    this.executionMetadata = new HashMap<>();
  }

  public Map<String, Object> getExecutionMetadata() {
    return Collections.unmodifiableMap(this.executionMetadata);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(JobExecutionState.class.getSimpleName())
            .add("jobExecution", this.jobExecution)
            .add("runningState", this.runningState)
            .add("stage", this.stage)
            .toString();
  }

  public void setRunningState(JobState.RunningState runningState) {
    switch (runningState){
      case PENDING: this.switchToPending(); break;
      case RUNNING: this.switchToRunning(); break;
      case SUCCESSFUL: this.switchToSuccessful(); break;
      case FAILED: this.switchToFailed(); break;
      case COMMITTED: this.switchToCommitted(); break;
      case CANCELLED: this.switchToCancelled(); break;
      default: throw new Error("Unexpected state");
    }
  }

  public void switchToPending() {
    Preconditions.checkState(null == this.runningState, "unexpected state:" + this.runningState);
    doRunningStateChange(RunningState.PENDING);
  }

  public void switchToRunning() {
    Preconditions.checkState(RunningState.PENDING == this.runningState,
        "unexpected state:" + this.runningState);
    doRunningStateChange(RunningState.RUNNING);
  }

  public void switchToSuccessful() {
    Preconditions.checkState(RunningState.RUNNING == this.runningState,
        "unexpected state:" + this.runningState);
    doRunningStateChange(RunningState.SUCCESSFUL);
  }

  public void switchToFailed() {
    Preconditions.checkState(RunningState.RUNNING == this.runningState,
        "unexpected state:" + this.runningState);
    doRunningStateChange(RunningState.FAILED);
  }

  public void switchToCommitted() {
    Preconditions.checkState(RunningState.SUCCESSFUL == this.runningState,
        "unexpected state:" + this.runningState);
    doRunningStateChange(RunningState.COMMITTED);
  }

  public void switchToCancelled() {
    Preconditions.checkState(RunningState.PENDING == this.runningState
        || RunningState.RUNNING == this.runningState
        || RunningState.SUCCESSFUL == this.runningState,
        "unexpected state:" + this.runningState);
    doRunningStateChange(RunningState.CANCELLED);
  }

  private void doRunningStateChange(RunningState newState) {
    RunningState oldState = this.runningState;
    this.runningState = newState;;
    if (this.listener.isPresent()) {
      this.listener.get().onStatusChange(this, oldState, this.runningState);
    }
  }

  public void setStage(String newStage) {
    String oldStage = this.stage;
    this.stage = newStage;
    if (this.listener.isPresent()) {
      this.listener.get().onStageTransition(this, oldStage, this.stage);
    }
  }

  public void setMedatata(String key, Object value) {
    Object oldValue = this.executionMetadata.get(key);
    this.executionMetadata.put(key, value);
    if (this.listener.isPresent()) {
      this.listener.get().onMetadataChange(this, key, oldValue, value);
    }
  }

}
