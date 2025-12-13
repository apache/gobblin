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

package org.apache.gobblin.temporal.ddm.activity;

import java.util.Map;
import java.util.Properties;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

import org.apache.gobblin.metrics.opentelemetry.GobblinOpenTelemetryMetrics;

/**
 * Temporal activity interface for emitting OpenTelemetry metrics.
 */
@ActivityInterface
public interface EmitOTelMetrics {

  /**
   * Emits a long counter metric.
   *
   * @param metric The OpenTelemetry metric to emit.
   * @param value The value to add to the counter.
   * @param attributes Additional attributes for the metric.
   * @param jobProps Properties related to the job context.
   */
  @ActivityMethod
  void emitLongCounterMetric(GobblinOpenTelemetryMetrics metric, long value, Map<String, String> attributes, Properties jobProps);

  /**
   * Emits a double histogram metric.
   *
   * @param metric The OpenTelemetry metric to emit.
   * @param value The value to record in the histogram.
   * @param attributes Additional attributes for the metric.
   * @param jobProps Properties related to the job context.
   */
  @ActivityMethod
  void emitDoubleHistogramMetric(GobblinOpenTelemetryMetrics metric, double value, Map<String, String> attributes, Properties jobProps);
}
