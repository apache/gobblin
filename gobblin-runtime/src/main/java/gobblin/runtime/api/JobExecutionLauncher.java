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
package gobblin.runtime.api;

import com.codahale.metrics.Gauge;

import gobblin.annotation.Alpha;
import gobblin.instrumented.Instrumentable;
import gobblin.metrics.ContextAwareCounter;
import gobblin.metrics.ContextAwareGauge;

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
    public static final String NUM_JOBS_LAUNCHED_COUNTER = "numJobsLaunched";
    public static final String NUM_JOBS_COMPLETED_COUNTER = "numJobsCompleted";
    public static final String NUM_JOBS_COMMITTED_COUNTER = "numJobsCommitted";
    public static final String NUM_JOBS_FAILED_COUNTER = "numJobsFailed";
    public static final String NUM_JOBS_CANCELLED_COUNTER = "numJobsCancelled";
    public static final String NUM_JOBS_RUNNING_GAUGE = "numJobsRunning";

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
      this.numJobsLaunched = parent.getMetricContext().contextAwareCounter(NUM_JOBS_LAUNCHED_COUNTER);
      this.numJobsCompleted = parent.getMetricContext().contextAwareCounter(NUM_JOBS_COMPLETED_COUNTER);
      this.numJobsCommitted = parent.getMetricContext().contextAwareCounter(NUM_JOBS_COMMITTED_COUNTER);
      this.numJobsFailed = parent.getMetricContext().contextAwareCounter(NUM_JOBS_FAILED_COUNTER);
      this.numJobsCancelled = parent.getMetricContext().contextAwareCounter(NUM_JOBS_CANCELLED_COUNTER);
      this.numJobsRunning = parent.getMetricContext().newContextAwareGauge(NUM_JOBS_RUNNING_GAUGE,
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
