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
package org.apache.gobblin.metrics.reporter.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;


public class KafkaReporterUtils {
  public static final String METRIC_REPORT_AVRO_SCHEMA_FILE = "MetricReport.avsc";
  public static final String GOBBLIN_TRACKING_EVENT_AVRO_SCHEMA_FILE = "GobblinTrackingEvent.avsc";

  private static final Splitter SPLIT_BY_COMMA = Splitter.on(",").omitEmptyStrings().trimResults();
  private static final Splitter SPLIT_BY_COLON = Splitter.on(":").omitEmptyStrings().trimResults();

  /***
   * This method extracts Map of namespaces to override in Kafka schema from Config.
   *
   * Example config:
   * kafka.schemaRegistry.overrideNamespace = namespace1:replacement1,namespace2:replacement2
   *
   * For the above example, this method will create a Map with values:
   * {
   *   "namespace1" : "replacement1",
   *   "namespace2" : "replacement2"
   * }
   *
   * @param properties Properties properties.
   * @return Map of namespace overrides.
   */
  public static Optional<Map<String, String>> extractOverrideNamespace(Properties properties) {
    if (properties.containsKey(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE)) {

      Map<String, String> namespaceOverridesMap = Maps.newHashMap();
      List<String> namespaceOverrides = Lists.newArrayList(SPLIT_BY_COMMA.split(properties
          .getProperty(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_OVERRIDE_NAMESPACE)));

      for (String namespaceOverride : namespaceOverrides) {
        List<String> override = Lists.newArrayList(SPLIT_BY_COLON.split(namespaceOverride));
        if (override.size() != 2) {
          throw new RuntimeException("Namespace override should be of the format originalNamespace:replacementNamespace,"
              + " found: " + namespaceOverride);
        }
        namespaceOverridesMap.put(override.get(0), override.get(1));
      }

      // If no entry found in the config value, mark it absent
      if (namespaceOverridesMap.size() != 0) {
        return Optional.of(namespaceOverridesMap);
      }
    }

    return Optional.<Map<String, String>>absent();
  }

  public static boolean isMetricsEnabled(Properties properties) {
    Optional<String> defaultTopic = getDefaultTopic(properties);
    Optional<String> metricsTopic = getMetricsTopic(properties);

    return metricsTopic.or(defaultTopic).isPresent();
  }

  public static boolean isEventsEnabled(Properties properties) {
    Optional<String> defaultTopic = getDefaultTopic(properties);
    Optional<String> eventsTopic = getEventsTopic(properties);

    return eventsTopic.or(defaultTopic).isPresent();
  }

  public static Optional<String> getDefaultTopic(Properties properties) {
    return Optional.fromNullable(properties.getProperty(ConfigurationKeys.METRICS_KAFKA_TOPIC));
  }

  public static Optional<String> getMetricsTopic(Properties properties) {
    return Optional.fromNullable(properties.getProperty(ConfigurationKeys.METRICS_KAFKA_TOPIC_METRICS));
  }

  public static Optional<String> getEventsTopic(Properties properties) {
    return Optional.fromNullable(properties.getProperty(ConfigurationKeys.METRICS_KAFKA_TOPIC_EVENTS));
  }

  public static boolean isKafkaReportingEnabled(Properties properties) {
    return Boolean.parseBoolean(
        properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_ENABLED_KEY, ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_ENABLED));
  }

  public static boolean isKafkaAvroSchemaRegistryEnabled(Properties properties) {
    return Boolean.parseBoolean(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY, ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY));
  }

  public static Schema getMetricReportSchema() throws IOException {
    return new Schema.Parser()
        .parse(KafkaReporterUtils.class.getClassLoader().getResourceAsStream(METRIC_REPORT_AVRO_SCHEMA_FILE));
  }

  public static Schema getGobblinTrackingEventSchema() throws IOException {
    return new Schema.Parser()
        .parse(KafkaReporterUtils.class.getClassLoader().getResourceAsStream(GOBBLIN_TRACKING_EVENT_AVRO_SCHEMA_FILE));
  }
}
