package org.apache.gobblin.metrics;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;


@Slf4j
public class InMemoryOpenTelemetryMetrics extends OpenTelemetryMetricsBase {
  public InMemoryOpenTelemetryMetrics(State state) {
    super(state);
  }

  protected MetricExporter initializeMetricExporter(State state) {
    return new LogBasedMetricExporter();
  }
  @Override
  protected void initialize(State state) {
    SdkMeterProvider meterProvider = SdkMeterProvider.builder()
        .setResource(Resource.getDefault())
        .registerMetricReader(
            PeriodicMetricReader.builder(this.metricExporter).build()
        )
        .build();

    this.openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
//    this.closer.register(this.opentelemetrySdk);
  }

  public InMemoryMetricExporter getInMemoryMetricExporter() {
    return (InMemoryMetricExporter) this.metricExporter;
  }

  static class LogBasedMetricExporter implements MetricExporter {
    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType instrumentType) {
      return AggregationTemporality.DELTA;
    }

    @Override
    public CompletableResultCode export(Collection<MetricData> metrics) {
      System.out.println("Exporting Otel metrics: " + metrics);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }
}
