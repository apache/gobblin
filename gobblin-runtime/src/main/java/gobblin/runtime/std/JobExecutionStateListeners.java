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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobExecutionStateListener;
import gobblin.runtime.api.JobExecutionStateListenerContainer;
import gobblin.util.callbacks.CallbacksDispatcher;

/**
 * A helper class to maintain a list of {@link JobExecutionStateListener} instances. It itself
 * implements the JobExecutionStateListener interface so all calls are dispatched to the children
 * listeners.
 */
public class JobExecutionStateListeners
    implements JobExecutionStateListener, JobExecutionStateListenerContainer, Closeable {
  private CallbacksDispatcher<JobExecutionStateListener> _dispatcher;

  public JobExecutionStateListeners(Optional<ExecutorService> execService,
                                    Optional<Logger> log) {
    _dispatcher = new CallbacksDispatcher<>(execService, log);
  }

  public JobExecutionStateListeners(Logger log) {
    _dispatcher = new CallbacksDispatcher<>(log);
  }

  public JobExecutionStateListeners() {
    _dispatcher = new CallbacksDispatcher<>();
  }

  /** {@inheritDoc} */
  @Override
  public void registerStateListener(JobExecutionStateListener listener) {
    _dispatcher.addListener(listener);
  }

  /** {@inheritDoc} */
  @Override
  public void unregisterStateListener(JobExecutionStateListener listener) {
    _dispatcher.removeListener(listener);
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

  @Override
  public void registerWeakStateListener(JobExecutionStateListener listener) {
    _dispatcher.addWeakListener(listener);
  }

  @Override
  public void close()
      throws IOException {
    _dispatcher.close();
  }
}
