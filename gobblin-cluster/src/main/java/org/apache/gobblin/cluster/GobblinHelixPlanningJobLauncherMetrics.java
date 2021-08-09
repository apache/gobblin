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

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.MetricContext;


public class GobblinHelixPlanningJobLauncherMetrics extends StandardMetricsBridge.StandardMetrics {
  private final String metricsName;
  public static final String TIMER_FOR_COMPLETED_PLANNING_JOBS = "timeForCompletedPlanningJobs";
  public static final String TIMER_FOR_FAILED_PLANNING_JOBS = "timeForFailedPlanningJobs";
  public static final String METER_FOR_SKIPPED_PLANNING_JOBS = "skippedPlanningJobs";

  final ContextAwareTimer timeForCompletedPlanningJobs;
  final ContextAwareTimer timeForFailedPlanningJobs;
  final ContextAwareMeter skippedPlanningJobs;

  public GobblinHelixPlanningJobLauncherMetrics(String metricsName,
      final MetricContext metricContext,
      int windowSizeInMin,
      HelixJobsMapping jobsMapping) {

    this.metricsName = metricsName;
    this.timeForCompletedPlanningJobs = metricContext.contextAwareTimer(TIMER_FOR_COMPLETED_PLANNING_JOBS, windowSizeInMin, TimeUnit.MINUTES);
    this.timeForFailedPlanningJobs = metricContext.contextAwareTimer(TIMER_FOR_FAILED_PLANNING_JOBS, windowSizeInMin, TimeUnit.MINUTES);
    this.skippedPlanningJobs = metricContext.contextAwareMeter(METER_FOR_SKIPPED_PLANNING_JOBS);
    this.contextAwareMetrics.add(timeForCompletedPlanningJobs);
    this.contextAwareMetrics.add(timeForFailedPlanningJobs);
  }

  public void updateTimeForCompletedPlanningJobs(long startTime) {
    Instrumented.updateTimer(
        com.google.common.base.Optional.of(this.timeForCompletedPlanningJobs),
        System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
  }

  public void updateTimeForFailedPlanningJobs(long startTime) {
    Instrumented.updateTimer(
        com.google.common.base.Optional.of(this.timeForFailedPlanningJobs),
        System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
  }

  @Override
  public String getName() {
    return this.metricsName;
  }
}
