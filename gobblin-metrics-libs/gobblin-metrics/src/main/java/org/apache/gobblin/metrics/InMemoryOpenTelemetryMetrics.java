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

package org.apache.gobblin.metrics;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.DefaultAggregationSelector;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

import org.apache.gobblin.configuration.State;


/**
 * A stub for OpenTelemetryMetrics that uses in-memory metric reader for testing purposes
 */
public class InMemoryOpenTelemetryMetrics extends OpenTelemetryMetricsBase {
  public InMemoryMetricReader metricReader;
  public InMemoryOpenTelemetryMetrics(State state) {
    super(state);
  }
  @Override
  void initialize(State state) {
    this.metricReader = InMemoryMetricReader.create(AggregationTemporalitySelector.deltaPreferred(),  DefaultAggregationSelector.getDefault());
    SdkMeterProvider meterProvider = SdkMeterProvider.builder()
        .registerMetricReader(this.metricReader)
        .build();
    this.openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
  }

  MetricExporter initializeMetricExporter(State state) {
    // Metrics are never exported if the metric reader is in-memory
    return null;
  }

  /**
   * Returns a new instance of {@link InMemoryOpenTelemetryMetrics} as it does not need to be globally shared
   * @param state
   * @return
   */
  public static InMemoryOpenTelemetryMetrics getInstance(State state) {
    return new InMemoryOpenTelemetryMetrics(state);
  }
}
