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
package gobblin.runtime.scheduler;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecSchedule;
import gobblin.runtime.api.JobSpecSchedulerListener;
import gobblin.runtime.api.JobSpecSchedulerListenersContainer;
import gobblin.util.callbacks.CallbacksDispatcher;

/**
 * Manages a list of {@link JobSpecSchedulerListener}s and can dispatch callbacks to them.
 */
public class JobSpecSchedulerListeners
       implements JobSpecSchedulerListenersContainer, JobSpecSchedulerListener, Closeable {
  private CallbacksDispatcher<JobSpecSchedulerListener> _dispatcher;

  public JobSpecSchedulerListeners(Optional<ExecutorService> execService,
                                    Optional<Logger> log) {
    _dispatcher = new CallbacksDispatcher<>(execService, log);
  }

  public JobSpecSchedulerListeners(Logger log) {
    _dispatcher = new CallbacksDispatcher<>(log);
  }

  public JobSpecSchedulerListeners() {
    _dispatcher = new CallbacksDispatcher<>();
  }

  /** {@inheritDoc} */
  @Override public void registerJobSpecSchedulerListener(JobSpecSchedulerListener listener) {
    _dispatcher.addListener(listener);
  }

  /** {@inheritDoc} */
  @Override public void unregisterJobSpecSchedulerListener(JobSpecSchedulerListener listener) {
    _dispatcher.removeListener(listener);
  }

  /** {@inheritDoc} */
  @Override public List<JobSpecSchedulerListener> getJobSpecSchedulerListeners() {
    return _dispatcher.getListeners();
  }

  @Override public void onJobScheduled(JobSpecSchedule jobSchedule) {
    try {
      _dispatcher.execCallbacks(new JobScheduledCallback(jobSchedule));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onJobScheduled interrupted.");
    }
  }

  @Override public void onJobTriggered(JobSpec jobSpec) {
    try {
      _dispatcher.execCallbacks(new JobTriggeredCallback(jobSpec));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onJobTriggered interrupted.");
    }
  }

  @Override public void onJobUnscheduled(JobSpecSchedule jobSchedule) {
    try {
      _dispatcher.execCallbacks(new JobUnscheduledCallback(jobSchedule));
    } catch (InterruptedException e) {
      _dispatcher.getLog().warn("onJobUnscheduled interrupted.");
    }
  }

  @Override
  public void registerWeakJobSpecSchedulerListener(JobSpecSchedulerListener listener) {
    _dispatcher.addWeakListener(listener);
  }

  @Override
  public void close()
      throws IOException {
    _dispatcher.close();
  }
}
