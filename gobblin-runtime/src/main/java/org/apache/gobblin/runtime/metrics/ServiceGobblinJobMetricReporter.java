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

package org.apache.gobblin.runtime.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.JobEvent;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;


/**
 * A metrics reporter to report job-level metrics to Gobblin-as-a-Service
 * Metrics should have the name FLOW_GROUP.FLOW_NAME.EDGE_ID.METRIC_NAME
 * If edge ID does not exist due to a different flowgraph being used, use the jobName as default
 */
public class ServiceGobblinJobMetricReporter implements GobblinJobMetricReporter {
  static String FLOW_EDGE_ID_KEY = "flow.edge.id";
  private Optional<MetricContext> metricContext;

  public ServiceGobblinJobMetricReporter(Optional<MetricContext> metricContext) {
    this.metricContext = metricContext;
  }

  public void reportWorkUnitCreationTimerMetrics(TimingEvent workUnitsCreationTimer, JobState jobState) {
    if (!this.metricContext.isPresent() || !jobState.getPropAsBoolean(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS, true)) {
      return;
    }
    String workunitCreationGaugeName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, jobState.getProp(ConfigurationKeys.FLOW_GROUP_KEY),
        jobState.getProp(ConfigurationKeys.FLOW_NAME_KEY), jobState.getProp(FLOW_EDGE_ID_KEY, jobState.getJobName()), TimingEvent.LauncherTimings.WORK_UNITS_CREATION);
    long workUnitsCreationTime = workUnitsCreationTimer.getDuration() / TimeUnit.SECONDS.toMillis(1);
    ContextAwareGauge<Integer> workunitCreationGauge =
        this.metricContext.get().newContextAwareGauge(workunitCreationGaugeName, () -> (int) workUnitsCreationTime);
    this.metricContext.get().register(workunitCreationGaugeName, workunitCreationGauge);
  }

  public void reportWorkUnitCountMetrics(int workUnitCount, JobState jobState) {
    if (!this.metricContext.isPresent() || !jobState.getPropAsBoolean(ConfigurationKeys.GOBBLIN_OUTPUT_JOB_LEVEL_METRICS, true)) {
      return;
    }
    String workunitCountGaugeName =  MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, jobState.getProp(ConfigurationKeys.FLOW_GROUP_KEY),
        jobState.getProp(ConfigurationKeys.FLOW_NAME_KEY), jobState.getProp(FLOW_EDGE_ID_KEY, jobState.getJobName()), JobEvent.WORK_UNITS_CREATED);
    ContextAwareGauge<Integer> workunitCountGauge = this.metricContext.get()
        .newContextAwareGauge(workunitCountGaugeName, () -> Integer.valueOf(workUnitCount));
    this.metricContext.get().register(workunitCountGaugeName, workunitCountGauge);
  }
}
