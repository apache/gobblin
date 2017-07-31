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
package org.apache.gobblin.runtime.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.JobState.RunningState;

import javax.annotation.Nonnull;
import lombok.Getter;

/**
 * TODO
 *
 */
@Alpha
public class JobExecutionState implements JobExecutionStatus {
  public static final String UKNOWN_STAGE = "unkown";
  private static final Map<RunningState, Set<RunningState>>
      EXPECTED_PRE_TRANSITION_STATES = ImmutableMap.<RunningState, Set<RunningState>>builder()
        .put(RunningState.PENDING, ImmutableSet.<RunningState>builder().build())
        .put(RunningState.RUNNING, ImmutableSet.<RunningState>builder().add(RunningState.PENDING).build())
        .put(RunningState.SUCCESSFUL, ImmutableSet.<RunningState>builder().add(RunningState.RUNNING).build())
        .put(RunningState.COMMITTED, ImmutableSet.<RunningState>builder().add(RunningState.SUCCESSFUL).build())
        .put(RunningState.FAILED,
             ImmutableSet.<RunningState>builder()
               .add(RunningState.PENDING).add(RunningState.RUNNING).add(RunningState.SUCCESSFUL).build())
        .put(RunningState.CANCELLED,
            ImmutableSet.<RunningState>builder()
              .add(RunningState.PENDING).add(RunningState.RUNNING).add(RunningState.SUCCESSFUL).build())
        .build();


  public static final Predicate<JobExecutionState> EXECUTION_DONE_PREDICATE =
      new Predicate<JobExecutionState>() {
        @Override public boolean apply(@Nonnull JobExecutionState state) {
          return null != state.getRunningState() && state.getRunningState().isDone();
        }
        @Override public String toString() {
          return "runningState().isDone()";
        }
      };

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
    doRunningStateChange(runningState);
  }

  public void switchToPending() {
    doRunningStateChange(RunningState.PENDING);
  }

  public void switchToRunning() {
    doRunningStateChange(RunningState.RUNNING);
  }

  public void switchToSuccessful() {
    doRunningStateChange(RunningState.SUCCESSFUL);
  }

  public void switchToFailed() {
    doRunningStateChange(RunningState.FAILED);
  }

  public void switchToCommitted() {
    doRunningStateChange(RunningState.COMMITTED);
  }

  public void switchToCancelled() {
    doRunningStateChange(RunningState.CANCELLED);
  }

  // This must be called only when holding changeLock
  private void doRunningStateChange(RunningState newState) {
    RunningState oldState = null;
    JobExecutionStateListener stateListener = null;
    this.changeLock.lock();
    try {
      // verify transition
      if (null == this.runningState) {
        Preconditions.checkState(RunningState.PENDING == newState);
      }
      else {
        Preconditions.checkState(EXPECTED_PRE_TRANSITION_STATES.get(newState).contains(this.runningState),
            "unexpected state transition " + this.runningState + " --> " + newState);
      }

      oldState = this.runningState;
      this.runningState = newState;
      if (this.listener.isPresent()) {
        stateListener = this.listener.get();
      }
      this.runningStateChanged.signalAll();
    }
    finally {
      this.changeLock.unlock();
    }
    if (null != stateListener) {
      stateListener.onStatusChange(this, oldState, newState);
    }
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
    awaitForStatePredicate(EXECUTION_DONE_PREDICATE, timeoutMs);
  }

  public void awaitForState(final RunningState targetState, long timeoutMs)
         throws InterruptedException, TimeoutException {
    awaitForStatePredicate(new Predicate<JobExecutionState>() {
      @Override public boolean apply(JobExecutionState state) {
        return null != state.getRunningState() && state.getRunningState().equals(targetState);
      }
      @Override public String toString() {
        return "runningState == " + targetState;
      }
    }, timeoutMs);
  }

  /**
   * Waits till a predicate on {@link #getRunningState()} becomes true or timeout is reached.
   *
   * @param predicate               the predicate to evaluate. Note that even though the predicate
   *                                is applied on the entire object, it will be evaluated only when
   *                                the running state changes.
   * @param timeoutMs               the number of milliseconds to wait for the predicate to become
   *                                true; 0 means wait forever.
   * @throws InterruptedException   if the waiting was interrupted
   * @throws TimeoutException       if we reached the timeout before the predicate was satisfied.
   */
  public void awaitForStatePredicate(Predicate<JobExecutionState> predicate, long timeoutMs)
         throws InterruptedException, TimeoutException {
    Preconditions.checkArgument(timeoutMs >= 0);
    if (0 == timeoutMs) {
      timeoutMs = Long.MAX_VALUE;
    }

    long startTimeMs = System.currentTimeMillis();
    long millisRemaining = timeoutMs;
    this.changeLock.lock();
    try {
      while (!predicate.apply(this) && millisRemaining > 0) {
        if (!this.runningStateChanged.await(millisRemaining, TimeUnit.MILLISECONDS)) {
          // Not necessary but shuts up FindBugs RV_RETURN_VALUE_IGNORED_BAD_PRACTICE
          break;
        }
        millisRemaining = timeoutMs - (System.currentTimeMillis() - startTimeMs);
      }

      if (!predicate.apply(this)) {
        throw new TimeoutException("Timeout waiting for state predicate: " + predicate);
      }
    }
    finally {
      this.changeLock.unlock();
    }
  }

}
