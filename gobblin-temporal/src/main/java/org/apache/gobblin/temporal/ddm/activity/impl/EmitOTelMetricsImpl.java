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

import org.apache.gobblin.metrics.opentelemetry.GaaSOpenTelemetryMetrics;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryDoubleHistogram;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryHelper;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryInstrumentation;
import org.apache.gobblin.metrics.opentelemetry.OpenTelemetryLongCounter;
import org.apache.gobblin.temporal.ddm.activity.EmitOTelMetrics;


public class EmitOTelMetricsImpl implements EmitOTelMetrics {

  @Override
  public void emitLongCounterMetric(GaaSOpenTelemetryMetrics metric, long value, Map<String, String> attributes, Properties jobProps) {
    OpenTelemetryLongCounter longCounter = (OpenTelemetryLongCounter) OpenTelemetryInstrumentation.getInstance(jobProps).getOrCreate(metric);
    longCounter.add(value, OpenTelemetryHelper.toOpenTelemetryAttributes(attributes));
  }

  @Override
  public void emitDoubleHistogramMetric(GaaSOpenTelemetryMetrics metric, double value, Map<String, String> attributes, Properties jobProps) {
    OpenTelemetryDoubleHistogram doubleHistogram = (OpenTelemetryDoubleHistogram) OpenTelemetryInstrumentation.getInstance(jobProps).getOrCreate(metric);
    doubleHistogram.record(value, OpenTelemetryHelper.toOpenTelemetryAttributes(attributes));
  }
}
