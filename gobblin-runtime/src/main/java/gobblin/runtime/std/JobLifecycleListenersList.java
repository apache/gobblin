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
package gobblin.runtime.std;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobCatalogListenersContainer;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobExecutionStateListener.MetadataChangeCallback;
import gobblin.runtime.api.JobExecutionStateListener.StageTransitionCallback;
import gobblin.runtime.api.JobExecutionStateListener.StatusChangeCallback;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobLifecycleListenersContainer;
import gobblin.runtime.api.JobSpecSchedulerListenersContainer;
import gobblin.util.callbacks.CallbacksDispatcher;

/**
 * A default implementation to manage a list of {@link JobLifecycleListener}
 *
 */
public class JobLifecycleListenersList implements JobLifecycleListenersContainer, Closeable {
  private final CallbacksDispatcher<JobLifecycleListener> _dispatcher;
  private final JobCatalogListenersContainer _jobCatalogDelegate;
  private final JobSpecSchedulerListenersContainer _jobSchedulerDelegate;

  public JobLifecycleListenersList(JobCatalogListenersContainer jobCatalogDelegate,
                                   JobSpecSchedulerListenersContainer jobSchedulerDelegate,
                                   Optional<ExecutorService> execService,
                                   Optional<Logger> log) {
    _dispatcher = new CallbacksDispatcher<>(execService, log);
    _jobCatalogDelegate = jobCatalogDelegate;
    _jobSchedulerDelegate = jobSchedulerDelegate;
  }

  public JobLifecycleListenersList(JobCatalogListenersContainer jobCatalogDelegate,
                                   JobSpecSchedulerListenersContainer jobSchedulerDelegate,
                                   Logger log) {
    this(jobCatalogDelegate, jobSchedulerDelegate,
        Optional.<ExecutorService>absent(), Optional.of(log));
  }

  public JobLifecycleListenersList(JobCatalogListenersContainer jobCatalogDelegate,
      JobSpecSchedulerListenersContainer jobSchedulerDelegate) {
    this(jobCatalogDelegate, jobSchedulerDelegate,
         Optional.<ExecutorService>absent(), Optional.<Logger>absent());
  }

  /** {@inheritDoc} */
  @Override
  public void registerJobLifecycleListener(JobLifecycleListener listener) {
    _dispatcher.addListener(listener);
    _jobCatalogDelegate.addListener(listener);
    _jobSchedulerDelegate.registerJobSpecSchedulerListener(listener);
  }

  /** {@inheritDoc} */
  @Override
  public void unregisterJobLifecycleListener(JobLifecycleListener listener) {
    _jobSchedulerDelegate.unregisterJobSpecSchedulerListener(listener);
    _jobCatalogDelegate.removeListener(listener);
    _dispatcher.removeListener(listener);
  }

  @Override
  public void close()
      throws IOException {
    _dispatcher.close();
  }

  /** {@inheritDoc} */
  @Override
  public List<JobLifecycleListener> getJobLifecycleListeners() {
    return _dispatcher.getListeners();
  }

  public void onStatusChange(JobExecutionState state, RunningState previousStatus,
                              RunningState newStatus) {
    try {
      _dispatcher.execCallbacks(new StatusChangeCallback(state, previousStatus, newStatus));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onStatusChange interrupted.");
    }
  }

  public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
    try {
      _dispatcher.execCallbacks(new StageTransitionCallback(state, previousStage, newStage));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onStageTransition interrupted.");
    }
  }

  public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
    try {
      _dispatcher.execCallbacks(new MetadataChangeCallback(state, key, oldValue, newValue));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onMetadataChange interrupted.");
    }
  }

  public void onJobLaunch(JobExecutionDriver driver) {
    try {
      _dispatcher.execCallbacks(new JobLifecycleListener.JobLaunchCallback(driver));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onJobLaunch interrupted.");
    }
  }

  @Override
  public void registerWeakJobLifecycleListener(JobLifecycleListener listener) {
    _dispatcher.addWeakListener(listener);
    _jobCatalogDelegate.registerWeakJobCatalogListener(listener);
    _jobSchedulerDelegate.registerWeakJobSpecSchedulerListener(listener);
  }

}
