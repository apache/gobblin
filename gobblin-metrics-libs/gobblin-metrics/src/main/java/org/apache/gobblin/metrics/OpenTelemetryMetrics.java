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

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

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
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.PropertiesUtils;
/**
 * A metrics reporter wrapper that uses the OpenTelemetry standard to emit metrics
 * Currently separated from the legacy codehale metrics as we need to maintain backwards compatibility, but eventually
 * can replace the old metrics system with tighter integrations once it's stable
 * Defaults to using the HTTP exporter where it expects an endpoint and optional headers in JSON string format
 */

@Slf4j
public class OpenTelemetryMetrics extends OpenTelemetryMetricsBase {

  private static OpenTelemetryMetrics GLOBAL_INSTANCE;
  private static final Long DEFAULT_OPENTELEMETRY_REPORTING_INTERVAL_MILLIS = 10000L;

  private OpenTelemetryMetrics(State state) {
    super(state);
  }

  @Override
  protected MetricExporter initializeMetricExporter(State state) {
    // TODO: Refactor the method to use a factory pattern for instantiating MetricExporter. Each MetricExporter
    //       type would have its own factory class, ensuring proper instantiation and handling specific configs.
    if (state.getPropAsBoolean(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_LOGEXPORTER_ENABLED,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_OPENTELEMETRY_LOGEXPORTER_ENABLED)) {
      try {
        log.info("Initializing opentelemetry LogExporter class");
        Class<?> clazz = Class.forName(state.getProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_LOGEXPORTER_CLASSNAME));
        Method instanceMethod = clazz.getMethod("instance");
        // Invoke the method to get the singleton instance
        return metricExporter = (MetricExporter) instanceMethod.invoke(null);
      } catch (Exception e) {
        log.error("Error occurred while instantiating opentelemetry LogExporter class", e);
      }
    }

    Preconditions.checkArgument(state.contains(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENDPOINT),
        "OpenTelemetry endpoint must be provided");
    OtlpHttpMetricExporterBuilder httpExporterBuilder = OtlpHttpMetricExporter.builder();
    httpExporterBuilder.setEndpoint(state.getProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENDPOINT));

    if (state.contains(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_HEADERS)) {
      Map<String, String> headers = parseHttpHeaders(state.getProp(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_HEADERS));
      for (Map.Entry<String, String> header : headers.entrySet()) {
        httpExporterBuilder.addHeader(header.getKey(), header.getValue());
      }
    }
    return httpExporterBuilder.build();
  }

  public static OpenTelemetryMetrics getInstance(State state) {
    if (state.getPropAsBoolean(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_ENABLED,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_OPENTELEMETRY_ENABLED) && GLOBAL_INSTANCE == null) {
      GLOBAL_INSTANCE = new OpenTelemetryMetrics(state);
    }
    return GLOBAL_INSTANCE;
  }

  @Override
  protected void initialize(State state) {
    log.info("Initializing OpenTelemetry metrics");
    Properties metricProps = PropertiesUtils.extractChildProperties(state.getProperties(),
        ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_CONFIGS_PREFIX);
    // Default to empty resource because default resource still populates some values
    Resource metricsResource = Resource.empty();
    if (metricProps.isEmpty()) {
      log.warn("No OpenTelemetry metrics properties found, sending empty resource");
    } else {
      AttributesBuilder attributesBuilder = Attributes.builder();
      for (String key : metricProps.stringPropertyNames()) {
        attributesBuilder.put(AttributeKey.stringKey(key), metricProps.getProperty(key));
      }
      metricsResource = Resource.getDefault().merge(Resource.create(attributesBuilder.build()));
    }
    SdkMeterProvider meterProvider = SdkMeterProvider.builder()
        .setResource(metricsResource)
        .registerMetricReader(
            PeriodicMetricReader.builder(this.metricExporter)
                .setInterval(Duration.ofMillis(
                    state.getPropAsLong(ConfigurationKeys.METRICS_REPORTING_OPENTELEMETRY_INTERVAL_MILLIS,
                        DEFAULT_OPENTELEMETRY_REPORTING_INTERVAL_MILLIS)))
                .build())
        .build();

    this.openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();
  }

  protected static Map<String, String> parseHttpHeaders(String headersString) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readValue(headersString, HashMap.class);
    } catch (Exception e) {
      String errMsg = "Failed to parse headers: " + headersString;
      log.error(errMsg, e);
      throw new RuntimeException(errMsg);
    }
  }
}
