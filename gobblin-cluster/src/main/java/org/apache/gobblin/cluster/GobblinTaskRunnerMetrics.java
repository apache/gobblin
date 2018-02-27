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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.ContextAwareMetric;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.TaskExecutor;

import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;


public class GobblinTaskRunnerMetrics {

  static class InProcessTaskRunnerMetrics extends StandardMetricsBridge.StandardMetrics {
    private TaskExecutor taskExecutor;
    private static String CURRENT_QUEUED_TASK_COUNT = "currentQueuedTaskCount";
    private static String HISTORICAL_QUEUED_TASK_COUNT = "historicalQueuedTaskCount";
    private static String QUEUED_TASK_COUNT = "queuedTaskCount";
    private static String CURRENT_QUEUED_TASK_TOTAL_TIME = "currentQueuedTaskTotalTime";
    private static String HISTORICAL_QUEUED_TASK_TOTAL_TIME = "historicalQueuedTaskTotalTime";
    private static String QUEUED_TASK_TOTAL_TIME = "queuedTaskTotalTime";
    private static String FAILED_TASK_COUNT = "failedTaskCount";
    private static String SUCCESSFUL_TASK_COUNT = "successfulTaskCount";
    private static String RUNNING_TASK_COUNT = "runningTaskCount";

    private final ContextAwareGauge<Long> currentQueuedTaskCount;
    private final ContextAwareGauge<Long> currentQueuedTaskTotalTime;
    private final ContextAwareGauge<Long> historicalQueuedTaskCount;
    private final ContextAwareGauge<Long> historicalQueuedTaskTotalTime;
    private final ContextAwareGauge<Long> queuedTaskCount;
    private final ContextAwareGauge<Long> queuedTaskTotalTime;
    private final ContextAwareGauge<Long> failedTaskCount;
    private final ContextAwareGauge<Long> successfulTaskCount;
    private final ContextAwareGauge<Long> runningTaskCount;

    public InProcessTaskRunnerMetrics (TaskExecutor executor, MetricContext context) {
      taskExecutor = executor;
      currentQueuedTaskCount = context.newContextAwareGauge(CURRENT_QUEUED_TASK_COUNT, ()->this.taskExecutor.getCurrentQueuedTaskCount().longValue());
      currentQueuedTaskTotalTime = context.newContextAwareGauge(CURRENT_QUEUED_TASK_TOTAL_TIME, ()->this.taskExecutor.getCurrentQueuedTaskTotalTime().longValue());
      historicalQueuedTaskCount = context.newContextAwareGauge(HISTORICAL_QUEUED_TASK_COUNT, ()->this.taskExecutor.getHistoricalQueuedTaskCount().longValue());
      historicalQueuedTaskTotalTime = context.newContextAwareGauge(HISTORICAL_QUEUED_TASK_TOTAL_TIME, ()->this.taskExecutor.getHistoricalQueuedTaskTotalTime().longValue());
      queuedTaskCount = context.newContextAwareGauge(QUEUED_TASK_COUNT, ()->this.taskExecutor.getQueuedTaskCount().longValue());
      queuedTaskTotalTime = context.newContextAwareGauge(QUEUED_TASK_TOTAL_TIME, ()->this.taskExecutor.getQueuedTaskTotalTime().longValue());
      failedTaskCount = context.newContextAwareGauge(FAILED_TASK_COUNT, ()->this.taskExecutor.getFailedTaskCount().getCount());
      successfulTaskCount = context.newContextAwareGauge(SUCCESSFUL_TASK_COUNT, ()->this.taskExecutor.getSuccessfulTaskCount().getCount());
      runningTaskCount = context.newContextAwareGauge(RUNNING_TASK_COUNT, ()->this.taskExecutor.getRunningTaskCount().getCount());
    }

    @Override
    public String getName() {
      return InProcessTaskRunnerMetrics.class.getName();
    }

    @Override
    public Collection<ContextAwareMetric> getContextAwareMetrics() {
      List list = Lists.newArrayList();
      list.add(currentQueuedTaskCount);
      list.add(currentQueuedTaskTotalTime);
      list.add(historicalQueuedTaskCount);
      list.add(historicalQueuedTaskTotalTime);
      list.add(queuedTaskCount);
      list.add(queuedTaskTotalTime);
      list.add(failedTaskCount);
      list.add(successfulTaskCount);
      list.add(runningTaskCount);
      return list;
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return ImmutableMap.of(ConfigurationKeys.WORK_UNIT_CREATION_AND_RUN_INTERVAL, this.taskExecutor.getTaskCreateAndRunTimer());
    }
  }

  static class JvmTaskRunnerMetrics extends StandardMetricsBridge.StandardMetrics {
    //TODO: add metrics to monitor the process execution status
    @Override
    public String getName() {
      return JvmTaskRunnerMetrics.class.getName();
    }

  }
}
