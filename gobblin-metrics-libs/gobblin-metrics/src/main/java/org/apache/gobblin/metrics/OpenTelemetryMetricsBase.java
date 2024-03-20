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

import java.io.IOException;

import com.google.common.io.Closer;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

import org.apache.gobblin.configuration.State;


public abstract class OpenTelemetryMetricsBase implements AutoCloseable {
  protected MetricExporter metricExporter;

  protected OpenTelemetry openTelemetry;

  Closer closer;

  public OpenTelemetryMetricsBase(State state) {
    this.closer = Closer.create();
    this.metricExporter = initializeMetricExporter(state);
    this.closer.register(this.metricExporter);
    initialize(state);
  }

  abstract MetricExporter initializeMetricExporter(State state);
  abstract void initialize(State state);

  public Meter getMeter(String groupName) {
    return this.openTelemetry.getMeterProvider().get(groupName);
  }

  public void close() throws IOException {
    if (this.closer != null) {
      this.closer.close();
    }
  }
}
