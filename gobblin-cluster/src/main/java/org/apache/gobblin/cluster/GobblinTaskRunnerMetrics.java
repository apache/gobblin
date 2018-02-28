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

import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.StandardMetricsBridge;
import org.apache.gobblin.metrics.ContextAwareMetric;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.TaskExecutor;


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

    public InProcessTaskRunnerMetrics (TaskExecutor executor, MetricContext context) {
      taskExecutor = executor;
      contextAwareMetrics.add(context.newContextAwareGauge(CURRENT_QUEUED_TASK_COUNT, ()->this.taskExecutor.getCurrentQueuedTaskCount().longValue()));
      contextAwareMetrics.add(context.newContextAwareGauge(CURRENT_QUEUED_TASK_TOTAL_TIME, ()->this.taskExecutor.getCurrentQueuedTaskTotalTime().longValue()));
      contextAwareMetrics.add(context.newContextAwareGauge(HISTORICAL_QUEUED_TASK_COUNT, ()->this.taskExecutor.getHistoricalQueuedTaskCount().longValue()));
      contextAwareMetrics.add(context.newContextAwareGauge(HISTORICAL_QUEUED_TASK_TOTAL_TIME, ()->this.taskExecutor.getHistoricalQueuedTaskTotalTime().longValue()));
      contextAwareMetrics.add(context.newContextAwareGauge(QUEUED_TASK_COUNT, ()->this.taskExecutor.getQueuedTaskCount().longValue()));
      contextAwareMetrics.add(context.newContextAwareGauge(QUEUED_TASK_TOTAL_TIME, ()->this.taskExecutor.getQueuedTaskTotalTime().longValue()));
      contextAwareMetrics.add(context.newContextAwareGauge(FAILED_TASK_COUNT, ()->this.taskExecutor.getFailedTaskCount().getCount()));
      contextAwareMetrics.add(context.newContextAwareGauge(SUCCESSFUL_TASK_COUNT, ()->this.taskExecutor.getSuccessfulTaskCount().getCount()));
      contextAwareMetrics.add(context.newContextAwareGauge(RUNNING_TASK_COUNT, ()->this.taskExecutor.getRunningTaskCount().getCount()));

      this.rawMetrics.put(ConfigurationKeys.WORK_UNIT_CREATION_AND_RUN_INTERVAL, this.taskExecutor.getTaskCreateAndRunTimer());
    }

    @Override
    public String getName() {
      return InProcessTaskRunnerMetrics.class.getName();
    }
  }

  static class JvmTaskRunnerMetrics extends StandardMetricsBridge.StandardMetrics {
    //TODO: add metrics to monitor the process execution status (will be revisited after process isolation work is done)
    @Override
    public String getName() {
      return JvmTaskRunnerMetrics.class.getName();
    }

  }
}
