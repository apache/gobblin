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

import org.slf4j.Logger;

import com.google.common.base.Optional;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobExecutionStateListener;

/**
 * Default NOOP implementation for a {@link JobExecutionStateListener}. The implementation can
 * optionally log the callbacks.
 */
public class DefaultJobExecutionStateListenerImpl implements JobExecutionStateListener {
  protected final Optional<Logger> _log;

  /**
   * Constructor
   * @param log   if present, the implementation will log each callback at INFO level
   */
  public DefaultJobExecutionStateListenerImpl(Optional<Logger> log) {
    _log = log;
  }

  public DefaultJobExecutionStateListenerImpl(Logger log) {
    this(Optional.of(log));
  }

  /** Constructor with no logging */
  public DefaultJobExecutionStateListenerImpl() {
    this(Optional.<Logger>absent());
  }

  /** {@inheritDoc} */
  @Override
  public void onStatusChange(JobExecutionState state, RunningState previousStatus, RunningState newStatus) {
    if (_log.isPresent()) {
      _log.get().info("JobExection status change for " + state.getJobSpec().toShortString() +
                      ": " + previousStatus + " --> " + newStatus);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
    if (_log.isPresent()) {
      _log.get().info("JobExection stage change for " + state.getJobSpec().toShortString() +
                      ": " + previousStage + " --> " + newStage);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onMetadataChange(JobExecutionState state, String key, Object oldValue,
                               Object newValue) {
    if (_log.isPresent()) {
      _log.get().info("JobExection metadata change for " + state.getJobSpec().toShortString() +
                      key + ": '" + oldValue + "' --> '" + newValue + "'");
    }
  }

}
