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

import java.util.concurrent.Future;

import com.codahale.metrics.Gauge;

import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMeter;


/**
 * A factory for {@link JobExecutionDriver}s.
 */
@Alpha
public interface JobExecutionLauncher extends Instrumentable {
  /**
   * This method is to launch the job specified by {@param jobSpec}
   * The simplest way is to run a {@link JobExecutionDriver} and upon completion return a
   * {@link org.apache.gobblin.runtime.job_exec.JobLauncherExecutionDriver.JobExecutionMonitorAndDriver}.
   *
   * If {@link JobExecutionDriver} does not run within the same process/node of {@link JobExecutionLauncher}, a simple monitoring
   * future object ({@link JobExecutionMonitor}) can be returned. This object can do two things:
   *
   * 1) Wait for computation of final {@link ExecutionResult} by invoking {@link Future#get()}.
   * 2) Monitor current job running status by invoking {@link JobExecutionMonitor#getRunningState()}.
   *
   * @see JobExecutionMonitor
   */
  JobExecutionMonitor launchJob(JobSpec jobSpec);

  /**
   * Common metrics for all launcher implementations.
   */
  StandardMetrics getMetrics();

  public static class StandardMetrics {
    public static final String NUM_JOBS_LAUNCHED = "numJobsLaunched";
    public static final String NUM_JOBS_COMPLETED = "numJobsCompleted";
    public static final String NUM_JOBS_COMMITTED = "numJobsCommitted";
    public static final String NUM_JOBS_FAILED = "numJobsFailed";
    public static final String NUM_JOBS_CANCELLED = "numJobsCancelled";
    public static final String NUM_JOBS_RUNNING = "numJobsRunning";
    public static final String TIMER_FOR_COMPLETED_JOBS = "timeForCompletedJobs";
    public static final String TIMER_FOR_FAILED_JOBS = "timeForFailedJobs";
    public static final String TIMER_FOR_COMMITTED_JOBS = "timerForCommittedJobs";

    public static final String EXECUTOR_ACTIVE_COUNT = "executorActiveCount";
    public static final String EXECUTOR_MAX_POOL_SIZE = "executorMaximumPoolSize";
    public static final String EXECUTOR_POOL_SIZE = "executorPoolSize";
    public static final String EXECUTOR_CORE_POOL_SIZE = "executorCorePoolSize";
    public static final String EXECUTOR_QUEUE_SIZE = "executorQueueSize";
    @Getter private final ContextAwareMeter numJobsLaunched;
    @Getter private final ContextAwareMeter numJobsCompleted;
    @Getter private final ContextAwareMeter numJobsCommitted;
    @Getter private final ContextAwareMeter numJobsFailed;
    @Getter private final ContextAwareMeter numJobsCancelled;
    @Getter private final ContextAwareGauge<Integer> numJobsRunning;

    public StandardMetrics(final JobExecutionLauncher parent) {
      this.numJobsLaunched = parent.getMetricContext().contextAwareMeter(NUM_JOBS_LAUNCHED);
      this.numJobsCompleted = parent.getMetricContext().contextAwareMeter(NUM_JOBS_COMPLETED);
      this.numJobsCommitted = parent.getMetricContext().contextAwareMeter(NUM_JOBS_COMMITTED);
      this.numJobsFailed = parent.getMetricContext().contextAwareMeter(NUM_JOBS_FAILED);
      this.numJobsCancelled = parent.getMetricContext().contextAwareMeter(NUM_JOBS_CANCELLED);
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
