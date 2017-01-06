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

import com.google.common.base.Predicate;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecSchedule;

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

  /**
   * {@inheritDoc}
   *
   *  NOTE: For this callback only conditions on the URI and version will be used.
   * */
  @Override public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
    JobSpec fakeJobSpec = JobSpec.builder(deletedJobURI).withVersion(deletedJobVersion).build();
    if (this.filter.apply(fakeJobSpec)) {
      this.delegate.onDeleteJob(deletedJobURI, deletedJobVersion);
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
  @Override public void onMetadataChange(JobExecutionState state, String key, Object oldValue,
                                         Object newValue) {
    if (this.filter.apply(state.getJobSpec())) {
      this.delegate.onMetadataChange(state, key, oldValue, newValue);
    }
  }

  @Override public void onJobScheduled(JobSpecSchedule jobSchedule) {
    if (this.filter.apply(jobSchedule.getJobSpec())) {
      this.delegate.onJobScheduled(jobSchedule);
    }
  }

  @Override public void onJobUnscheduled(JobSpecSchedule jobSchedule) {
    if (this.filter.apply(jobSchedule.getJobSpec())) {
      this.delegate.onJobUnscheduled(jobSchedule);
    }
  }

  @Override public void onJobTriggered(JobSpec jobSpec) {
    if (this.filter.apply(jobSpec)) {
      this.delegate.onJobTriggered(jobSpec);
    }
  }

  @Override public void onJobLaunch(JobExecutionDriver jobDriver) {
    if (this.filter.apply(jobDriver.getJobExecutionState().getJobSpec())) {
      this.delegate.onJobLaunch(jobDriver);
    }

  }

}
