package org.apache.gobblin.metrics;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;


public class OpenTelemetryMetricsTest  {

  @Test
  void testInitializeOpenTelemetryFailsWithoutEndpoint() {
    State opentelemetryState = new State();
    opentelemetryState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED, "true");
    Assert.assertThrows(IllegalArgumentException.class, () -> {
      OpenTelemetryMetrics.getInstance(opentelemetryState);
    });
  }

  @Test
  void testInitializeOpenTelemetrySucceedsWithEndpoint() {
    State opentelemetryState = new State();
    opentelemetryState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED, "true");
    opentelemetryState.setProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENDPOINT, "http://localhost:4317");
    OpenTelemetryMetrics metricsProvider = OpenTelemetryMetrics.getInstance(opentelemetryState);
    System.out.println(metricsProvider.metricExporter.toString());
  }

  @Test
  void testHeadersParseCorrectly() {
    Map<String, String> headers = OpenTelemetryMetrics.parseHttpHeaders(
        "{\"Content-Type\":\"application/x-protobuf\",\"headerTag\":\"tag1:value1,tag2:value2\"}");
    Assert.assertEquals(headers.size(), 2);
    Assert.assertEquals(headers.get("Content-Type"), "application/x-protobuf");
    Assert.assertEquals(headers.get("headerTag"), "tag1:value1,tag2:value2");
  }

}
