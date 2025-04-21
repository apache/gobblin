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

package org.apache.gobblin.temporal.workflows.metrics;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import com.uber.m3.tally.StatsReporter;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.registry.otlp.AggregationTemporality;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.temporal.common.reporter.MicrometerClientStatsReporter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
public class TemporalMetricsHelper {

  /**
   * Retrieves a map of dimension names and their corresponding values from the provided config.
   * The dimensions are defined as a comma-separated string in the config, and the method
   * fetches the corresponding values for each dimension.
   * A missing dimension in config will have empty string as value.
   *
   * @param config Config object
   * @return a map where the key is the dimension name and the value is the corresponding value from the config
   */
  public static Map<String, String> getDimensions(Config config) {
    String dimensionsString = ConfigUtils.getString(config, GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_OTLP_DIMENSIONS_KEY, "");

    // Split the string by "," and create a map by fetching values from config
    return Arrays.stream(dimensionsString.split(","))
        .map(String::trim)
        .filter(StringUtils::isNotBlank)
        .collect(Collectors.toMap(key -> key, key -> ConfigUtils.getString(config, key, ""), (l, r)-> r));
  }

  /**
   * Creates and returns a {@link StatsReporter} instance configured with an {@link OtlpMeterRegistry}.
   * This reporter can be used to report metrics via the OpenTelemetry Protocol (OTLP) to a metrics backend.
   *
   * @param config Config object
   * @return a {@link StatsReporter} instance, configured with an OTLP meter registry and ready to report metrics.
   */
  public static StatsReporter getStatsReporter(Config config) {
    OtlpConfig otlpConfig = getOtlpConfig(config);
    MeterRegistry meterRegistry = new OtlpMeterRegistry(otlpConfig, Clock.SYSTEM);
    return new MicrometerClientStatsReporter(meterRegistry);
  }

  @VisibleForTesting
  static OtlpConfig getOtlpConfig(Config config) {
    return new OtlpConfig() {
      @Override
      public String get(@NotNull String key) {
        return ConfigUtils.getString(config, key, null);
      }

      @NotNull
      @Override
      public String prefix() {
        return GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_OTLP_PREFIX_WITHOUT_DOT;
      }

      @NotNull
      @Override
      public AggregationTemporality aggregationTemporality() {
        return AggregationTemporality.DELTA;
      }

      @NotNull
      @Override
      public Map<String, String> headers() {
        String headers = get(GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_OTLP_HEADERS_KEY);
        return parseHeaders(headers);
      }

      @NotNull
      @Override
      public Duration step() {
        int reportInterval = ConfigUtils.getInt(config, GobblinTemporalConfigurationKeys.TEMPORAL_METRICS_REPORT_INTERVAL_SECS,
            GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_METRICS_REPORT_INTERVAL_SECS);
        return Duration.ofSeconds(reportInterval);
      }
    };
  }

  private static Map<String, String> parseHeaders(String headersString) {
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
