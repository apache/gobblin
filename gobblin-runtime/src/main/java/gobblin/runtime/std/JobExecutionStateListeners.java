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

import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobExecutionStateListener;
import gobblin.runtime.api.JobExecutionStateListenerContainer;
import gobblin.util.callbacks.Callback;
import gobblin.util.callbacks.CallbackFactory;
import gobblin.util.callbacks.CallbacksDispatcher;

/**
 * A helper class to maintain a list of {@link JobExecutionStateListener} instances. It itself
 * implements the JobExecutionStateListener interface so all calls are dispatched to the children
 * listeners.
 */
public class JobExecutionStateListeners
    implements JobExecutionStateListener, JobExecutionStateListenerContainer {
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
  public void onStatusChange(JobExecutionState state, RunningState previousStatus, RunningState newStatus) {
    try {
      _dispatcher.execCallbacks(new StatusChangeCallbackFactory(state, previousStatus,
          newStatus));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onStatusChange interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
    try {
      _dispatcher.execCallbacks(new StageTransitionCallbackFactory(state, previousStage, newStage));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onStageTransition interrupted.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
    try {
      _dispatcher.execCallbacks(new MetadataChangeCallbackFactory(state, key, oldValue, newValue));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onMetadataChange interrupted.");
    }
  }

  protected static class StatusChangeCallback extends Callback<JobExecutionStateListener, Void> {
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

  protected static class StatusChangeCallbackFactory
            implements CallbackFactory<JobExecutionStateListener, Void> {
    private final Function<JobExecutionStateListener, Void> _callback;

    public StatusChangeCallbackFactory(final JobExecutionState state,
                                       final RunningState previousStatus,
                                       final RunningState newStatus) {
      _callback = new StatusChangeCallback(state, previousStatus, newStatus);
    }

    @Override public Function<JobExecutionStateListener, Void> createCallbackRunnable() {
      return _callback;
    }
  }

  protected static class StageTransitionCallback extends
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

  protected static class StageTransitionCallbackFactory
            implements CallbackFactory<JobExecutionStateListener, Void> {
    private final Function<JobExecutionStateListener, Void> _callback;

    public StageTransitionCallbackFactory(final JobExecutionState state, final String previousStage,
                                          final String newStage) {
      _callback = new StageTransitionCallback(state, previousStage, newStage);
    }

    @Override public Function<JobExecutionStateListener, Void> createCallbackRunnable() {
      return _callback;
    }
  }

  protected static class MetadataChangeCallback extends Callback<JobExecutionStateListener, Void> {
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

  protected static class MetadataChangeCallbackFactory
            implements CallbackFactory<JobExecutionStateListener, Void> {
    private final Function<JobExecutionStateListener, Void> _callback;

    public MetadataChangeCallbackFactory(final JobExecutionState state, final String key,
                                         final Object oldValue, final Object newValue) {
      _callback = new MetadataChangeCallback(state, key, oldValue, newValue);
    }

    @Override public Function<JobExecutionStateListener, Void> createCallbackRunnable() {
      return _callback;
    }
  }

}
