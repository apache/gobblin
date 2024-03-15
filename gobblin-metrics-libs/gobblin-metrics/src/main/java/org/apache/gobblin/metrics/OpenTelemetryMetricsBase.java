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
