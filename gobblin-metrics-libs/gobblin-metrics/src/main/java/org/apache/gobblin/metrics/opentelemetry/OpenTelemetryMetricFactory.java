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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.Meter;

/**
 * Factory interface for creating OpenTelemetry metrics of type {@link T}.
 *
 * @param <T> the type of OpenTelemetry metric to be created
 */
public interface OpenTelemetryMetricFactory<T extends OpenTelemetryMetric> {

  OpenTelemetryMetricFactory<OpenTelemetryLongCounter> LONG_COUNTER_FACTORY = new OpenTelemetryLongCounterFactory();
  OpenTelemetryMetricFactory<OpenTelemetryDoubleHistogram> DOUBLE_HISTOGRAM_FACTORY = new OpenTelemetryDoubleHistogramFactory();

  T newMetric(String name, String description, String unit, Attributes attributes, Meter meter);

  /** Factory class for creating {@link OpenTelemetryLongCounter} metrics. */
  class OpenTelemetryLongCounterFactory implements OpenTelemetryMetricFactory<OpenTelemetryLongCounter> {

    @Override
    public OpenTelemetryLongCounter newMetric(String name, String description, String unit, Attributes attributes, Meter meter) {
      return new OpenTelemetryLongCounter(name, attributes,
          meter.counterBuilder(name).setDescription(description).setUnit(unit).build());
    }

  }

  /** Factory class for creating {@link OpenTelemetryDoubleHistogram} metrics. */
  class OpenTelemetryDoubleHistogramFactory implements OpenTelemetryMetricFactory<OpenTelemetryDoubleHistogram> {

    @Override
    public OpenTelemetryDoubleHistogram newMetric(String name, String description, String unit, Attributes attributes, Meter meter) {
      return new OpenTelemetryDoubleHistogram(name, attributes,
          meter.histogramBuilder(name).setDescription(description).setUnit(unit).build());
    }

  }
}
