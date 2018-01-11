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

import com.codahale.metrics.Gauge;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareGauge;

import lombok.Getter;

/**
 * A factory for {@link JobExecutionDriver}s.
 */
@Alpha
public interface JobExecutionLauncher extends Instrumentable {
  JobExecutionDriver launchJob(JobSpec jobSpec);

  /**
   * Common metrics for all launcher implementations. */
  StandardMetrics getMetrics();

  public static class StandardMetrics {
    public static final String NUM_JOBS_LAUNCHED = "numJobsLaunched";
    public static final String NUM_JOBS_COMPLETED = "numJobsCompleted";
    public static final String NUM_JOBS_COMMITTED = "numJobsCommitted";
    public static final String NUM_JOBS_FAILED = "numJobsFailed";
    public static final String NUM_JOBS_CANCELLED = "numJobsCancelled";
    public static final String NUM_JOBS_RUNNING = "numJobsRunning";

    public static final String TIMER_FOR_JOB_COMPLETION = "timerForJobCompletion";
    public static final String TIMER_FOR_JOB_FAILURE = "timerForJobFailure";
    public static final String TIMER_BEFORE_JOB_SCHEDULING = "timerBeforeJobScheduling";
    public static final String TIMER_BEFORE_JOB_LAUNCHING = "timerBeforeJobLaunching";

    public static final String TRACKING_EVENT_NAME = "JobExecutionLauncherEvent";
    public static final String JOB_EXECID_META = "jobExecId";
    public static final String JOB_LAUNCHED_OPERATION_TYPE = "JobLaunched";
    public static final String JOB_COMPLETED_OPERATION_TYPE = "JobCompleted";
    public static final String JOB_COMMITED_OPERATION_TYPE = "JobCommitted";
    public static final String JOB_FAILED_OPERATION_TYPE = "JobFailed";
    public static final String JOB_CANCELLED_OPERATION_TYPE = "JobCancelled";

    @Getter private final ContextAwareCounter numJobsLaunched;
    @Getter private final ContextAwareCounter numJobsCompleted;
    @Getter private final ContextAwareCounter numJobsCommitted;
    @Getter private final ContextAwareCounter numJobsFailed;
    @Getter private final ContextAwareCounter numJobsCancelled;
    @Getter private final ContextAwareGauge<Integer> numJobsRunning;

    public StandardMetrics(final JobExecutionLauncher parent) {
      this.numJobsLaunched = parent.getMetricContext().contextAwareCounter(NUM_JOBS_LAUNCHED);
      this.numJobsCompleted = parent.getMetricContext().contextAwareCounter(NUM_JOBS_COMPLETED);
      this.numJobsCommitted = parent.getMetricContext().contextAwareCounter(NUM_JOBS_COMMITTED);
      this.numJobsFailed = parent.getMetricContext().contextAwareCounter(NUM_JOBS_FAILED);
      this.numJobsCancelled = parent.getMetricContext().contextAwareCounter(NUM_JOBS_CANCELLED);
      this.numJobsRunning = parent.getMetricContext().newContextAwareGauge(NUM_JOBS_RUNNING,
            new Gauge<Integer>() {
              @Override public Integer getValue() {
                return (int)(StandardMetrics.this.getNumJobsLaunched().getCount() -
                       StandardMetrics.this.getNumJobsCompleted().getCount() -
                       StandardMetrics.this.getNumJobsCancelled().getCount());
              }
      });
    }
  }
}
