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

package org.apache.gobblin.metrics.opentelemetry;

import lombok.AllArgsConstructor;
import lombok.Getter;


@Getter
@AllArgsConstructor
public enum GobblinOpenTelemetryMetrics {
  /**
   * Metric to track the count of Gobblin Jobs for each of its state (GenerateWorkUnit, ProcessWorkUnit, CommitStep).
   * Metric Unit: 1 represents each increment will add one data point to the counter.
   * */
  GOBBLIN_JOB_STATE("gobblin_job_state", "Gobblin job state counter", "1", OpenTelemetryMetricType.LONG_COUNTER),

  /**
   * Metric to track the latency of each Gobblin Job state (GenerateWorkUnit, ProcessWorkUnit, CommitStep).
   * Metric Unit: seconds (s) represents the time taken for each state.
   * */
  GOBBLIN_JOB_STATE_LATENCY("gobblin_job_state_latency", "Gobblin job state latency", "s", OpenTelemetryMetricType.DOUBLE_HISTOGRAM);

  private final String metricName;
  private final String metricDescription;
  private final String metricUnit;
  private final OpenTelemetryMetricType metricType;

  @Override
  public String toString() {
    return String.format("Metric{name='%s', description='%s', unit='%s', type=%s}", metricName, metricDescription, metricUnit, metricType);
  }

}
