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

package org.apache.gobblin.cluster;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.listeners.AbstractJobListener;
import org.apache.gobblin.runtime.listeners.JobListener;


/**
 * A job listener used when {@link GobblinHelixJobLauncher} launches a job.
 * The {@link GobblinHelixJobLauncherMetrics} will always be passed in because
 * it will be be updated accordingly.
 */
class GobblinHelixJobLauncherListener extends AbstractJobListener {

  private final GobblinHelixJobLauncherMetrics jobLauncherMetrics;
  private static final String JOB_START_TIME = "jobStartTime";

  GobblinHelixJobLauncherListener(GobblinHelixJobLauncherMetrics jobLauncherMetrics) {
    this.jobLauncherMetrics = jobLauncherMetrics;
  }

  @Override
  public void onJobPrepare(JobContext jobContext)
      throws Exception {
    super.onJobPrepare(jobContext);
    jobContext.getJobState().setProp(JOB_START_TIME, Long.toString(System.nanoTime()));
    jobLauncherMetrics.numJobsLaunched.mark();
  }

  /**
   * From {@link org.apache.gobblin.runtime.AbstractJobLauncher#launchJob(JobListener)}, the final
   * job state should only be FAILED or COMMITTED. This means the completed jobs metrics covers
   * both failed jobs and committed jobs.
   */
  @Override
  public void onJobCompletion(JobContext jobContext)
      throws Exception {
    super.onJobCompletion(jobContext);
    long startTime = jobContext.getJobState().getPropAsLong(JOB_START_TIME);
    jobLauncherMetrics.numJobsCompleted.mark();
    Instrumented.updateTimer(Optional.of(jobLauncherMetrics.timeForCompletedJobs), System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    if (jobContext.getJobState().getState() == JobState.RunningState.FAILED) {
      jobLauncherMetrics.numJobsFailed.mark();
      Instrumented.updateTimer(Optional.of(jobLauncherMetrics.timeForFailedJobs), System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    } else {
      jobLauncherMetrics.numJobsCommitted.mark();
      Instrumented.updateTimer(Optional.of(jobLauncherMetrics.timeForCommittedJobs), System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    }
  }

  @Override
  public void onJobCancellation(JobContext jobContext)
      throws Exception {
    super.onJobCancellation(jobContext);
    jobLauncherMetrics.numJobsCancelled.mark();
  }
}
