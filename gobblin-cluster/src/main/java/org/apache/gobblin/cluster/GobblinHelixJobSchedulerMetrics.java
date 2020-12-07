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

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareTimer;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;


class GobblinHelixJobSchedulerMetrics extends StandardMetricsBridge.StandardMetrics {
  public static final String SCHEDULE_CANCELLATION_START = "scheduleCancellationStart";
  public static final String SCHEDULE_CANCELLATION_END = "scheduleCancellationEnd";
  public static final String TIMER_BEFORE_JOB_SCHEDULING = "timerBeforeJobScheduling";
  public static final String TIMER_BEFORE_JOB_LAUNCHING = "timerBeforeJobLaunching";
  public static final String TIMER_BETWEEN_JOB_SCHEDULING_AND_LAUNCHING = "timerBetwenJobSchedulingAndLaunching";

  final AtomicLong numCancellationStart;
  final AtomicLong numCancellationComplete;
  final ContextAwareTimer timeBeforeJobScheduling;
  final ContextAwareTimer timeBeforeJobLaunching;
  final ContextAwareTimer timeBetwenJobSchedulingAndLaunching;

  final ThreadPoolExecutor threadPoolExecutor;

  public GobblinHelixJobSchedulerMetrics (final ExecutorService jobExecutor, final MetricContext metricContext, int windowSizeInMin) {
    this.timeBeforeJobScheduling = metricContext.contextAwareTimer(TIMER_BEFORE_JOB_SCHEDULING,
        windowSizeInMin, TimeUnit.MINUTES);
    this.timeBeforeJobLaunching = metricContext.contextAwareTimer(TIMER_BEFORE_JOB_LAUNCHING,
        windowSizeInMin, TimeUnit.MINUTES);
    this.timeBetwenJobSchedulingAndLaunching = metricContext.contextAwareTimer(TIMER_BETWEEN_JOB_SCHEDULING_AND_LAUNCHING,
        windowSizeInMin, TimeUnit.MINUTES);
    this.numCancellationStart = new AtomicLong(0);
    this.numCancellationComplete = new AtomicLong(0);

    this.contextAwareMetrics.add(metricContext.newContextAwareGauge(SCHEDULE_CANCELLATION_START, ()->this.numCancellationStart.get()));
    this.contextAwareMetrics.add(metricContext.newContextAwareGauge(SCHEDULE_CANCELLATION_END, ()->this.numCancellationComplete.get()));
    this.contextAwareMetrics.add(timeBeforeJobScheduling);
    this.contextAwareMetrics.add(timeBeforeJobLaunching);
    this.contextAwareMetrics.add(timeBetwenJobSchedulingAndLaunching);

    this.threadPoolExecutor = (ThreadPoolExecutor) jobExecutor;

    // executor metrics
    this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.EXECUTOR_ACTIVE_COUNT, ()->this.threadPoolExecutor.getActiveCount()));
    this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.EXECUTOR_MAX_POOL_SIZE, ()->this.threadPoolExecutor.getMaximumPoolSize()));
    this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.EXECUTOR_POOL_SIZE, ()->this.threadPoolExecutor.getPoolSize()));
    this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.EXECUTOR_CORE_POOL_SIZE, ()->this.threadPoolExecutor.getCorePoolSize()));
    this.contextAwareMetrics.add(metricContext.newContextAwareGauge(JobExecutionLauncher.StandardMetrics.EXECUTOR_QUEUE_SIZE, ()->this.threadPoolExecutor.getQueue().size()));
  }

  void updateTimeBeforeJobScheduling (Properties jobProps) {
    long jobCreationTime = Long.parseLong(jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "0"));
    Instrumented.updateTimer(Optional.of(timeBeforeJobScheduling),
        System.currentTimeMillis() - jobCreationTime,
        TimeUnit.MILLISECONDS);
  }

  void updateTimeBeforeJobLaunching (Properties jobProps) {
    long jobCreationTime = Long.parseLong(jobProps.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "0"));
    Instrumented.updateTimer(Optional.of(timeBeforeJobLaunching),
        System.currentTimeMillis() - jobCreationTime,
        TimeUnit.MILLISECONDS);
  }

  void updateTimeBetweenJobSchedulingAndJobLaunching (long scheduledTime, long launchingTime) {
    Instrumented.updateTimer(Optional.of(timeBetwenJobSchedulingAndLaunching),
        launchingTime - scheduledTime,
        TimeUnit.MILLISECONDS);
  }
}
