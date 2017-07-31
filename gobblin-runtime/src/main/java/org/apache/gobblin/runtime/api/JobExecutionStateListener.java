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

import com.google.common.base.Objects;

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.JobState.RunningState;
import org.apache.gobblin.util.callbacks.Callback;

/**
 * Notification for changes in the execution state
 *
 */
public interface JobExecutionStateListener {
  void onStatusChange(JobExecutionState state, JobState.RunningState previousStatus, JobState.RunningState newStatus);
  void onStageTransition(JobExecutionState state, String previousStage, String newStage);
  void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue);

  /** A standard implementation of {@link JobExecutionStateListener#onStatusChange(JobExecutionState, RunningState, RunningState)}
   * as a functional object. */
  static class StatusChangeCallback extends Callback<JobExecutionStateListener, Void> {
    private final JobExecutionState state;
    private final RunningState previousStatus;
    private final RunningState newStatus;

    public StatusChangeCallback(final JobExecutionState state,
                                final RunningState previousStatus,
                                final RunningState newStatus) {
      super(Objects.toStringHelper("onStatusChange")
                   .add("state", state)
                   .add("previousStatus", previousStatus)
                   .add("newStatus", newStatus)
                   .toString());
      this.state = state;
      this.previousStatus = previousStatus;
      this.newStatus = newStatus;
    }

    @Override public Void apply(JobExecutionStateListener input) {
      input.onStatusChange(this.state, this.previousStatus, this.newStatus);
      return null;
    }
  }

  /** A standard implementation of {@link JobExecutionStateListener#onStageTransition(JobExecutionState, String, String)}
   * as a functional object. */
  static class StageTransitionCallback extends
            Callback<JobExecutionStateListener, Void> {
    private final JobExecutionState state;
    private final String previousStage;
    private final String newStage;

    public StageTransitionCallback(final JobExecutionState state, final String previousStage,
                                   final String newStage) {
      super(Objects.toStringHelper("onStageTransition")
                   .add("state", state)
                   .add("previousStage", previousStage)
                   .add("newStage", newStage)
                   .toString());
      this.state = state;
      this.previousStage = previousStage;
      this.newStage = newStage;
    }

    @Override
    public Void apply(JobExecutionStateListener input) {
      input.onStageTransition(this.state, this.previousStage, this.newStage);
      return null;
    }
  }

  /** A standard implementation of {@link JobExecutionStateListener#onMetadataChange(JobExecutionState, String, Object, Object)}
   * as a functional object. */
  static class MetadataChangeCallback extends Callback<JobExecutionStateListener, Void> {
    private final JobExecutionState state;
    private final String key;
    private final Object oldValue;
    private final Object newValue;

    public MetadataChangeCallback(final JobExecutionState state, final String key,
                                  final Object oldValue, final Object newValue) {
      super(Objects.toStringHelper("onMetadataChange")
                   .add("state", state)
                   .add("key", key)
                   .add("oldValue", oldValue)
                   .add("newValue", newValue)
                   .toString());
      this.state = state;
      this.key = key;
      this.oldValue = oldValue;
      this.newValue = newValue;
    }

    @Override
    public Void apply(JobExecutionStateListener input) {
      input.onMetadataChange(state, key, oldValue, newValue);
      return null;
    }

  }
}
