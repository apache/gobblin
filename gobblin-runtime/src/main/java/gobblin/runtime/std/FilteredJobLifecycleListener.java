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
package gobblin.runtime.std;

import com.google.common.base.Predicate;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobSpec;

import lombok.AllArgsConstructor;

/**
 * A listener that can delegate to another {@link JobLifecycleListener} only callbacks that
 * are relevant to a specific Jobs.
 *
 */
@AllArgsConstructor
public class FilteredJobLifecycleListener implements JobLifecycleListener {
  private final Predicate<JobSpec> filter;
  private final JobLifecycleListener delegate;

  /** {@inheritDoc} */
  @Override public void onAddJob(JobSpec addedJob) {
    if (this.filter.apply(addedJob)) {
      this.delegate.onAddJob(addedJob);
    }
  }

  /** {@inheritDoc} */
  @Override public void onDeleteJob(JobSpec deletedJob) {
    if (this.filter.apply(deletedJob)) {
      this.delegate.onDeleteJob(deletedJob);
    }
  }

  /** {@inheritDoc} */
  @Override public void onUpdateJob(JobSpec updatedJob) {
    if (this.filter.apply(updatedJob)) {
      this.delegate.onUpdateJob(updatedJob);
    }
  }

  /** {@inheritDoc} */
  @Override public void onStatusChange(JobExecutionState state, RunningState previousStatus, RunningState newStatus) {
    if (this.filter.apply(state.getJobSpec())) {
      this.delegate.onStatusChange(state, previousStatus, newStatus);
    }
  }

  /** {@inheritDoc} */
  @Override public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
    if (this.filter.apply(state.getJobSpec())) {
      this.delegate.onStageTransition(state, previousStage, newStage);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
    if (this.filter.apply(state.getJobSpec())) {
      this.delegate.onMetadataChange(state, key, oldValue, newValue);
    }
  }

}
