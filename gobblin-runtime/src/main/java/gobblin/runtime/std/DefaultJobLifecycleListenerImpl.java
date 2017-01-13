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

import java.net.URI;

import org.slf4j.Logger;

import com.google.common.base.Optional;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecSchedule;

/**
 * Default NOOP implementation for {@link JobLifecycleListener}. It can log the callbacks. Other
 * implementing classes can use this as a base to override only the methods they care about.
 */
public class DefaultJobLifecycleListenerImpl implements JobLifecycleListener {
  protected final Optional<Logger> _log;

  /**
   * Constructor
   * @param log   if no log is specified, the logging will be done. Logging level is INFO.
   */
  public DefaultJobLifecycleListenerImpl(Optional<Logger> log) {
    _log = log;
  }

  public DefaultJobLifecycleListenerImpl(Logger log) {
    this(Optional.of(log));
  }

  /** Constructor with no logging */
  public DefaultJobLifecycleListenerImpl() {
    this(Optional.<Logger>absent());
  }

  /** {@inheritDoc} */
  @Override public void onAddJob(JobSpec addedJob) {
    if (_log.isPresent()) {
      _log.get().info("New JobSpec detected: " + addedJob.toShortString());
    }
  }

  /** {@inheritDoc} */
  @Override public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
    if (_log.isPresent()) {
      _log.get().info("JobSpec deleted: " + deletedJobURI + "/" + deletedJobVersion);
    }
  }

  /** {@inheritDoc} */
  @Override public void onUpdateJob(JobSpec updatedJob) {
    if (_log.isPresent()) {
      _log.get().info("JobSpec changed: " +  updatedJob.toShortString());
    }
  }

  /** {@inheritDoc} */
  @Override public void onStatusChange(JobExecutionState state, RunningState previousStatus,
                                       RunningState newStatus) {
    if (_log.isPresent()) {
      _log.get().info("JobExection status change for " + state.getJobSpec().toShortString() +
                      ": " + previousStatus + " --> " + newStatus);
    }
  }

  /** {@inheritDoc} */
  @Override public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
    if (_log.isPresent()) {
      _log.get().info("JobExection stage change for " + state.getJobSpec().toShortString() +
                      ": " + previousStage + " --> " + newStage);
    }
  }

  /** {@inheritDoc} */
  @Override public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
    if (_log.isPresent()) {
      _log.get().info("JobExection metadata change for " + state.getJobSpec().toShortString() +
                      key + ": '" + oldValue + "' --> '" + newValue + "'");
    }
  }

  @Override public void onJobScheduled(JobSpecSchedule jobSchedule) {
    if (_log.isPresent()) {
      _log.get().info("job scheduled: " + jobSchedule);
    }
  }

  @Override public void onJobUnscheduled(JobSpecSchedule jobSchedule) {
    if (_log.isPresent()) {
      _log.get().info("job unscheduled: " + jobSchedule);
    }
  }

  @Override public void onJobTriggered(JobSpec jobSpec) {
    if (_log.isPresent()) {
      _log.get().info("job triggered: " + jobSpec);
    }
  }

  @Override public void onJobLaunch(JobExecutionDriver jobDriver) {
    if (_log.isPresent()) {
      _log.get().info("job launched: " + jobDriver);
    }
  }

}
