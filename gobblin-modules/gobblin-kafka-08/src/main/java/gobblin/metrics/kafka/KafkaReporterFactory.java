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

package gobblin.metrics.kafka;

import java.io.IOException;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.CustomCodahaleReporterFactory;
import gobblin.metrics.KafkaReportingFormats;
import gobblin.metrics.MetricContext;
import gobblin.metrics.RootMetricContext;


@Slf4j
public class KafkaReporterFactory implements CustomCodahaleReporterFactory {
  @Override
  public ScheduledReporter newScheduledReporter(MetricRegistry registry, Properties properties)
      throws IOException {
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_ENABLED))) {
      return null;
    }
    log.info("Reporting metrics to Kafka");

    Optional<String> defaultTopic = Optional.fromNullable(properties.getProperty(ConfigurationKeys.METRICS_KAFKA_TOPIC));
    Optional<String> metricsTopic = Optional.fromNullable(
        properties.getProperty(ConfigurationKeys.METRICS_KAFKA_TOPIC_METRICS));
    Optional<String> eventsTopic = Optional.fromNullable(
        properties.getProperty(ConfigurationKeys.METRICS_KAFKA_TOPIC_EVENTS));

    boolean metricsEnabled = metricsTopic.or(defaultTopic).isPresent();
    if (metricsEnabled) log.info("Reporting metrics to Kafka");
    boolean eventsEnabled = eventsTopic.or(defaultTopic).isPresent();
    if (eventsEnabled) log.info("Reporting events to Kafka");

    try {
      Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.METRICS_KAFKA_BROKERS),
          "Kafka metrics brokers missing.");
      Preconditions.checkArgument(metricsTopic.or(eventsTopic).or(defaultTopic).isPresent(), "Kafka topic missing.");
    } catch (IllegalArgumentException exception) {
      log.error("Not reporting metrics to Kafka due to missing Kafka configuration(s).", exception);
      return null;
    }

    String brokers = properties.getProperty(ConfigurationKeys.METRICS_KAFKA_BROKERS);

    String reportingFormat = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_FORMAT,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_FORMAT);

    KafkaReportingFormats formatEnum;
    try {
      formatEnum = KafkaReportingFormats.valueOf(reportingFormat.toUpperCase());
    } catch (IllegalArgumentException exception) {
      log.warn("Kafka metrics reporting format " + reportingFormat +
          " not recognized. Will report in json format.", exception);
      formatEnum = KafkaReportingFormats.JSON;
    }

    if (metricsEnabled) {
      try {
        formatEnum.metricReporterBuilder(properties)
            .build(brokers, metricsTopic.or(defaultTopic).get(), properties);
      } catch (IOException exception) {
        log.error("Failed to create Kafka metrics reporter. Will not report metrics to Kafka.", exception);
      }
    }

    if (eventsEnabled) {
      try {
        KafkaEventReporter.Builder<?> builder = formatEnum.eventReporterBuilder(RootMetricContext.get(),
            properties);
        return builder.build(brokers, eventsTopic.or(defaultTopic).get());
      } catch (IOException exception) {
        log.error("Failed to create Kafka events reporter. Will not report events to Kafka.", exception);
      }
    }

    log.info("Will start reporting metrics to Kafka");
    return null;
  }
}
