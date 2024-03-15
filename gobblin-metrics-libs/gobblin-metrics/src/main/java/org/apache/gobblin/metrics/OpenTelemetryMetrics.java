package org.apache.gobblin.metrics;

import java.time.Duration;
import java.util.Properties;

import com.google.common.base.Optional;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporterBuilder;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.PropertiesUtils;
/**
 * A metrics reporter wrapper that uses the OpenTelemetry standard to emit metrics
 * Currently separated from the legacy codehale metrics as we need to maintain backwards compatibility, but eventually
 * can replace the old metrics system with tighter integrations once it's stable
 */
public class OpenTelemetryMetrics extends OpenTelemetryMetricsBase {

  private static OpenTelemetryMetrics GLOBAL_INSTANCE;

  private OpenTelemetryMetrics(State state) {
    super(state);
  }

  @Override
  protected MetricExporter initializeMetricExporter(State state) {
    OtlpHttpMetricExporterBuilder httpExporterBuilder = OtlpHttpMetricExporter.builder();
    httpExporterBuilder.setEndpoint(state.getProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENDPOINT));
    return httpExporterBuilder.build();
  }

  public static OpenTelemetryMetrics getInstance(State state) {
    if (state.getPropAsBoolean(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED, ConfigurationKeys.DEFAULT_METRICS_REPORTING_OPENTELEMETRY_ENABLED)) {
      if (GLOBAL_INSTANCE == null) {
        GLOBAL_INSTANCE = new OpenTelemetryMetrics(state);
      }
    }
    return GLOBAL_INSTANCE;
  }

  @Override
  protected void initialize(State state) {
    Properties metricProps = PropertiesUtils.extractPropertiesWithPrefix(state.getProperties(), Optional.of(
        ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_CONFIGS));
    AttributesBuilder attributesBuilder = Attributes.builder();
    for (String key : metricProps.stringPropertyNames()) {
      attributesBuilder.put(AttributeKey.stringKey(key), metricProps.getProperty(key));
    }
    Resource metricsResource = Resource.getDefault().merge(Resource.create(attributesBuilder.build()));

    SdkMeterProvider meterProvider = SdkMeterProvider.builder()
        .setResource(metricsResource)
        .registerMetricReader(
            PeriodicMetricReader.builder(this.metricExporter)
                .setInterval(Duration.ofMillis(state.getPropAsLong(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_INTERVAL_MILLIS)))
                .build())
        .build();

    this.openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal();
  }
}
