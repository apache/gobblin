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

import org.apache.gobblin.util.callbacks.Callback;

/**
 * {@link JobSpecScheduler} callbacks.
 */
public interface JobSpecSchedulerListener {
  /** Called just before a job gets added to the scheduler */
  void onJobScheduled(JobSpecSchedule jobSchedule);

  /** Called just before a job gets removed from the scheduler */
  void onJobUnscheduled(JobSpecSchedule jobSchedule);

  /** Called just before the job (runnable) gets triggered by the scheduler */
  void onJobTriggered(JobSpec jobSpec);

  public class JobScheduledCallback extends Callback<JobSpecSchedulerListener, Void> {
    private final JobSpecSchedule _jobSchedule;

    public JobScheduledCallback(JobSpecSchedule jobSchedule) {
      super(Objects.toStringHelper("onJobScheduled").add("jobSchedule", jobSchedule).toString());
      _jobSchedule = jobSchedule;
    }

    @Override public Void apply(JobSpecSchedulerListener listener) {
      listener.onJobScheduled(_jobSchedule);
      return null;
    }
  }

  public class JobUnscheduledCallback extends Callback<JobSpecSchedulerListener, Void> {
    private final JobSpecSchedule _jobSchedule;

    public JobUnscheduledCallback(JobSpecSchedule jobSchedule) {
      super(Objects.toStringHelper("onJobUnscheduled").add("jobSchedule", jobSchedule).toString());
      _jobSchedule = jobSchedule;
    }

    @Override public Void apply(JobSpecSchedulerListener listener) {
      listener.onJobUnscheduled(_jobSchedule);
      return null;
    }
  }

  public class JobTriggeredCallback extends Callback<JobSpecSchedulerListener, Void> {
    private final JobSpec _jobSpec;

    public JobTriggeredCallback(JobSpec jobSpec) {
      super(Objects.toStringHelper("onJobTriggered").add("jobSpec", jobSpec).toString());
      _jobSpec = jobSpec;
    }

    @Override public Void apply(JobSpecSchedulerListener listener) {
      listener.onJobTriggered(_jobSpec);
      return null;
    }
  }
}
