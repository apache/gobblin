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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
  @Getter final JobSpec jobSpec;
  // We use a lock instead of synchronized so that we can add different conditional variables if
  // needed.
  private final Lock changeLock = new ReentrantLock();
  private final Condition runningStateChanged = changeLock.newCondition();
  private JobState.RunningState runningState;
  /** Arbitrary execution stage, e.g. setup, workUnitGeneration, taskExecution, publishing */
  private String stage;
  private Map<String, Object> executionMetadata;

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
    this.changeLock.lock();
    try {
      return Collections.unmodifiableMap(this.executionMetadata);
    }
    finally {
      this.changeLock.unlock();
    }
  }

  @Override public String toString() {
    this.changeLock.lock();
    try {
      return Objects.toStringHelper(JobExecutionState.class.getSimpleName())
              .add("jobExecution", this.jobExecution)
              .add("runningState", this.runningState)
              .add("stage", this.stage)
              .toString();
    }
    finally {
      this.changeLock.unlock();
    }
  }

  @Override public JobState.RunningState getRunningState() {
    this.changeLock.lock();
    try {
      return this.runningState;
    }
    finally {
      this.changeLock.unlock();
    }
  }

  @Override
  public String getStage() {
    this.changeLock.lock();
    try {
      return this.stage;
    }
    finally {
      this.changeLock.unlock();
    }
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
    this.changeLock.lock();
    try {
      Preconditions.checkState(null == this.runningState, "unexpected state:" + this.runningState);
      doRunningStateChange(RunningState.PENDING);
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void switchToRunning() {
    this.changeLock.lock();
    try {
      Preconditions.checkState(RunningState.PENDING == this.runningState,
          "unexpected state:" + this.runningState);
      doRunningStateChange(RunningState.RUNNING);
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void switchToSuccessful() {
    this.changeLock.lock();
    try {
      Preconditions.checkState(RunningState.RUNNING == this.runningState,
          "unexpected state:" + this.runningState);
      doRunningStateChange(RunningState.SUCCESSFUL);
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void switchToFailed() {
    this.changeLock.lock();
    try {
      Preconditions.checkState(RunningState.PENDING == this.runningState // error during initialization
          || RunningState.RUNNING == this.runningState // error during execution
          || RunningState.SUCCESSFUL == this.runningState // error during commit
          ,
          "unexpected state:" + this.runningState);
      doRunningStateChange(RunningState.FAILED);
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void switchToCommitted() {
    this.changeLock.lock();
    try {
      Preconditions.checkState(RunningState.SUCCESSFUL == this.runningState,
          "unexpected state:" + this.runningState);
      doRunningStateChange(RunningState.COMMITTED);
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void switchToCancelled() {
    this.changeLock.lock();
    try {
      Preconditions.checkState(RunningState.PENDING == this.runningState
          || RunningState.RUNNING == this.runningState
          || RunningState.SUCCESSFUL == this.runningState,
          "unexpected state:" + this.runningState);
      doRunningStateChange(RunningState.CANCELLED);
    }
    finally {
      this.changeLock.unlock();
    }
  }

  // This must be called only when holding changeLock
  private void doRunningStateChange(RunningState newState) {
    RunningState oldState = this.runningState;
    this.runningState = newState;;
    if (this.listener.isPresent()) {
      this.listener.get().onStatusChange(this, oldState, this.runningState);
    }
    this.runningStateChanged.signalAll();
  }

  public void setStage(String newStage) {
    this.changeLock.lock();
    try {
      String oldStage = this.stage;
      this.stage = newStage;
      if (this.listener.isPresent()) {
        this.listener.get().onStageTransition(this, oldStage, this.stage);
      }
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void setMedatata(String key, Object value) {
    this.changeLock.lock();
    try {
      Object oldValue = this.executionMetadata.get(key);
      this.executionMetadata.put(key, value);
      if (this.listener.isPresent()) {
        this.listener.get().onMetadataChange(this, key, oldValue, value);
      }
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void awaitForDone(long timeoutMs) throws InterruptedException, TimeoutException {
    Preconditions.checkArgument(timeoutMs >= 0);
    if (0 == timeoutMs) {
      timeoutMs = Long.MAX_VALUE;
    }

    long startTimeMs = System.currentTimeMillis();
    long millisRemaining = timeoutMs;
    this.changeLock.lock();
    try {
      while ((null == this.runningState || !this.runningState.isDone()) && millisRemaining > 0) {
        this.runningStateChanged.await(millisRemaining, TimeUnit.MILLISECONDS);
        millisRemaining = timeoutMs - (System.currentTimeMillis() - startTimeMs);
      }

      if (null == this.runningState || !this.runningState.isDone()) {
        throw new TimeoutException("Timeout waiting for done.");
      }
    }
    finally {
      this.changeLock.unlock();
    }
  }

  public void awaitForState(RunningState targetState, long timeoutMs)
         throws InterruptedException, TimeoutException {
    Preconditions.checkArgument(timeoutMs >= 0);
    if (0 == timeoutMs) {
      timeoutMs = Long.MAX_VALUE;
    }

    long startTimeMs = System.currentTimeMillis();
    long millisRemaining = timeoutMs;
    this.changeLock.lock();
    try {
      while ((null == this.runningState || !this.runningState.equals(targetState)) && millisRemaining > 0) {
        this.runningStateChanged.await(millisRemaining, TimeUnit.MILLISECONDS);
        millisRemaining = timeoutMs - (System.currentTimeMillis() - startTimeMs);
      }

      if (null == this.runningState || !this.runningState.equals(targetState)) {
        throw new TimeoutException("Timeout waiting for state: " + targetState);
      }
    }
    finally {
      this.changeLock.unlock();
    }
  }

}
