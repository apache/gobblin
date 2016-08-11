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

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobLifecycleListenersContainer;
import gobblin.runtime.api.JobSpec;
import gobblin.util.callbacks.CallbacksDispatcher;

/**
 * A default implementation to manage a list of {@link JobLifecycleListener}
 *
 */
public class JobLifecycleListenersList implements JobLifecycleListener, JobLifecycleListenersContainer {
  private final CallbacksDispatcher<JobLifecycleListener> _dispatcher;

  public JobLifecycleListenersList(Optional<ExecutorService> execService,
                                    Optional<Logger> log) {
    _dispatcher = new CallbacksDispatcher<>(execService, log);
  }

  public JobLifecycleListenersList(Logger log) {
    _dispatcher = new CallbacksDispatcher<>(log);
  }

  public JobLifecycleListenersList() {
    _dispatcher = new CallbacksDispatcher<>();
  }

  /** {@inheritDoc} */
  @Override
  public void onAddJob(JobSpec addedJob) {
    Preconditions.checkNotNull(addedJob);
    try {
      _dispatcher.execCallbacks(new AddJobCallback(addedJob));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onAddJob interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onDeleteJob(JobSpec deletedJob) {
    Preconditions.checkNotNull(deletedJob);
    try {
      _dispatcher.execCallbacks(new DeleteJobCallback(deletedJob));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onDeleteJob interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onUpdateJob(JobSpec updatedJob) {
    Preconditions.checkNotNull(updatedJob);
    try {
      _dispatcher.execCallbacks(new UpdateJobCallback(updatedJob));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onUpdateJob interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onStatusChange(JobExecutionState state, RunningState previousStatus,
      RunningState newStatus) {
    try {
      _dispatcher.execCallbacks(new StatusChangeCallback(state, previousStatus, newStatus));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onStatusChange interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
    try {
      _dispatcher.execCallbacks(new StageTransitionCallback(state, previousStage, newStage));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onStageTransition interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
    try {
      _dispatcher.execCallbacks(new MetadataChangeCallback(state, key, oldValue, newValue));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onMetadataChange interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void registerJobLifecycleListener(JobLifecycleListener listener) {
    _dispatcher.addListener(listener);
  }

  /** {@inheritDoc} */
  @Override
  public void unregisterJobLifecycleListener(JobLifecycleListener listener) {
    _dispatcher.removeListener(listener);
  }

  /** {@inheritDoc} */
  @Override
  public List<JobLifecycleListener> getJobLifecycleListeners() {
    return _dispatcher.getListeners();
  }

}
