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

import lombok.Getter;

/**
 * Enum representing the types of OpenTelemetry metrics supported.
 */
@Getter
public enum OpenTelemetryMetricType {
  /** Represents a metric of type LongCounter. */
  LONG_COUNTER(OpenTelemetryMetricFactory.LONG_COUNTER_FACTORY),
  /** Represents a metric of type DoubleHistogram. */
  DOUBLE_HISTOGRAM(OpenTelemetryMetricFactory.DOUBLE_HISTOGRAM_FACTORY);

  private final OpenTelemetryMetricFactory<? extends OpenTelemetryMetric> factory;

  OpenTelemetryMetricType(OpenTelemetryMetricFactory<? extends OpenTelemetryMetric> factory) {
    this.factory = factory;
  }

}
