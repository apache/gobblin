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
import com.google.common.base.Preconditions;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.util.callbacks.Callback;

/**
 * A listener that can track the full lifecycle of a job: from the registering of the jobspec to
 * completion of any of the job executions.
 */
@Alpha
public interface JobLifecycleListener
        extends JobCatalogListener, JobSpecSchedulerListener, JobExecutionStateListener {
  /** Called before the job driver gets started. */
  void onJobLaunch(JobExecutionDriver jobDriver);

  public static class JobLaunchCallback extends Callback<JobLifecycleListener, Void> {
    private final JobExecutionDriver _jobDriver;

    public JobLaunchCallback(JobExecutionDriver jobDriver) {
      super(Objects.toStringHelper("onJobLaunch").add("jobDriver", jobDriver).toString());
      Preconditions.checkNotNull(jobDriver);
      _jobDriver = jobDriver;
    }

    @Override public Void apply(JobLifecycleListener listener) {
      listener.onJobLaunch(_jobDriver);
      return null;
    }

  }
}
