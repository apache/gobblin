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
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;


/**
 * A metrics reporter that reports only workunitsCreationTimer - which is the current default behavior for all Gobblin jobs not emitted by GaaS
 * Emit metrics with JobMetrics as the prefix
 */
public class DefaultGobblinJobMetricReporter implements GobblinJobMetricReporter {

  private Optional<MetricContext> metricContext;

  public DefaultGobblinJobMetricReporter(Optional<MetricContext> metricContext) {
     this.metricContext = metricContext;
  }

  public void reportWorkUnitCreationTimerMetrics(TimingEvent workUnitsCreationTimer, JobState jobState) {
    if (!this.metricContext.isPresent()) {
      return;
    }
    String workunitCreationGaugeName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_JOB_METRICS_PREFIX,
        TimingEvent.LauncherTimings.WORK_UNITS_CREATION, jobState.getJobName());
    long workUnitsCreationTime = workUnitsCreationTimer.getDuration() / TimeUnit.SECONDS.toMillis(1);
    ContextAwareGauge<Integer> workunitCreationGauge = this.metricContext.get()
        .newContextAwareGauge(workunitCreationGaugeName, () -> (int) workUnitsCreationTime);
    this.metricContext.get().register(workunitCreationGaugeName, workunitCreationGauge);
  }

  public void reportWorkUnitCountMetrics(int workUnitCount, JobState jobstate) {
    return;
  }

}
