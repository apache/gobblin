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

package org.apache.gobblin.temporal.ddm.activity.impl;

import java.util.Map;
import java.util.Properties;

import org.apache.gobblin.metrics.opentelemetry.GobblinOpenTelemetryMetrics;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryDoubleHistogram;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryHelper;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryInstrumentation;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryLongCounter;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryMetricType;
import org.apache.gobblin.temporal.ddm.activity.EmitOTelMetrics;

/**
 * Implementation of {@link EmitOTelMetrics} that emits OpenTelemetry metrics.
 */
public class EmitOTelMetricsImpl implements EmitOTelMetrics {

  @Override
  public void emitLongCounterMetric(GobblinOpenTelemetryMetrics metric, long value, Map<String, String> attributes, Properties jobProps) {
    if (!metric.getMetricType().equals(OpenTelemetryMetricType.LONG_COUNTER)) {
      throw new IllegalArgumentException("Metric " + metric.getMetricName() + " is not of type LONG_COUNTER");
    }
    OpenTelemetryLongCounter longCounter = OpenTelemetryInstrumentation.getInstance(jobProps).getOrCreate(metric);
    longCounter.add(value, OpenTelemetryHelper.toOpenTelemetryAttributes(attributes));
  }

  @Override
  public void emitDoubleHistogramMetric(GobblinOpenTelemetryMetrics metric, double value, Map<String, String> attributes, Properties jobProps) {
    if (!metric.getMetricType().equals(OpenTelemetryMetricType.DOUBLE_HISTOGRAM)) {
      throw new IllegalArgumentException("Metric " + metric.getMetricName() + " is not of type DOUBLE_HISTOGRAM");
    }
    OpenTelemetryDoubleHistogram doubleHistogram = OpenTelemetryInstrumentation.getInstance(jobProps).getOrCreate(metric);
    doubleHistogram.record(value, OpenTelemetryHelper.toOpenTelemetryAttributes(attributes));
  }
}
