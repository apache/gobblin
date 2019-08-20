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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;

/**
 * Metrics that relates to jobs launched by {@link GobblinHelixJobLauncher}.
 */
class GobblinHelixJobLauncherMetrics extends StandardMetricsBridge.StandardMetrics {
    private final String metricsName;
    final AtomicLong totalJobsLaunched;
    final AtomicLong totalJobsCompleted;
    final AtomicLong totalJobsCommitted;
    final AtomicLong totalJobsFailed;
    final AtomicLong totalJobsCancelled;

    final ContextAwareTimer timeForCompletedJobs;
    final ContextAwareTimer timeForFailedJobs;
    final ContextAwareTimer timeForCommittedJobs;

    public GobblinHelixJobLauncherMetrics(String metricsName, final MetricContext metricContext, int windowSizeInMin) {
      this.metricsName = metricsName;

      // All historical counters
      this.totalJobsLaunched = new AtomicLong(0);
      this.totalJobsCompleted = new AtomicLong(0);
      this.totalJobsCommitted = new AtomicLong(0);
      this.totalJobsFailed = new AtomicLong(0);
      this.totalJobsCancelled = new AtomicLong(0);

      this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_LAUNCHED, ()->this.totalJobsLaunched.get()));
      this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMPLETED, ()->this.totalJobsCompleted.get()));
      this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_COMMITTED, ()->this.totalJobsCommitted.get()));
      this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_FAILED, ()->this.totalJobsFailed.get()));
      this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_CANCELLED, ()->this.totalJobsCancelled.get()));
      this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.NUM_JOBS_RUNNING,
          ()->(int)(GobblinHelixJobLauncherMetrics.this.totalJobsLaunched.get() - GobblinHelixJobLauncherMetrics.this.totalJobsCompleted.get())));

      this.timeForCompletedJobs = metricContext.contextAwareTimer(JobExecutionLauncher.StandardMetrics.TIMER_FOR_COMPLETED_JOBS, windowSizeInMin, TimeUnit.MINUTES);
      this.timeForFailedJobs = metricContext.contextAwareTimer(JobExecutionLauncher.StandardMetrics.TIMER_FOR_FAILED_JOBS, windowSizeInMin, TimeUnit.MINUTES);
      this.timeForCommittedJobs = metricContext.contextAwareTimer(JobExecutionLauncher.StandardMetrics.TIMER_FOR_COMMITTED_JOBS, windowSizeInMin, TimeUnit.MINUTES);

      this.contextAwareMetrics.add(timeForCommittedJobs);
      this.contextAwareMetrics.add(timeForCompletedJobs);
      this.contextAwareMetrics.add(timeForFailedJobs);
    }

    @Override
    public String getName() {
      return this.metricsName;
    }
}
