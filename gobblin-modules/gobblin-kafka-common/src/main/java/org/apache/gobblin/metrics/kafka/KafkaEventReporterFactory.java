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
package org.apache.gobblin.metrics.kafka;

import java.io.IOException;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.CustomCodahaleReporterFactory;
import org.apache.gobblin.metrics.KafkaReportingFormats;
import org.apache.gobblin.metrics.MetricReporterException;
import org.apache.gobblin.metrics.ReporterSinkType;
import org.apache.gobblin.metrics.ReporterType;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.reporter.util.KafkaReporterUtils;

@Slf4j
public class KafkaEventReporterFactory implements CustomCodahaleReporterFactory {
  @Override
  public ScheduledReporter newScheduledReporter(MetricRegistry registry, Properties properties)
      throws IOException {
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_ENABLED_KEY,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_ENABLED))) {
      return null;
    }

    boolean eventsEnabled = KafkaReporterUtils.isEventsEnabled(properties);
    if (KafkaReporterUtils.isEventsEnabled(properties)) {
      log.info("Events enabled --- Reporting events to Kafka");
    }

    Optional<String> eventsTopic = KafkaReporterUtils.getEventsTopic(properties);
    Optional<String> defaultTopic = KafkaReporterUtils.getDefaultTopic(properties);

    try {
      Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.METRICS_KAFKA_BROKERS),
          "Kafka metrics brokers missing.");
      Preconditions.checkArgument(eventsTopic.or(defaultTopic).isPresent(), "Kafka topic missing.");
    } catch (IllegalArgumentException exception) {
      throw new MetricReporterException("Missing Kafka configuration(s).", exception, ReporterType.EVENT, ReporterSinkType.KAFKA);
    }

    String brokers = properties.getProperty(ConfigurationKeys.METRICS_KAFKA_BROKERS);

    String metricsReportingFormat = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_FORMAT,
        ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_FORMAT);

    KafkaReportingFormats formatEnum;
    try {
      formatEnum = KafkaReportingFormats.valueOf(metricsReportingFormat.toUpperCase());
    } catch (IllegalArgumentException exception) {
      log.warn(
          "Kafka metrics reporting format " + metricsReportingFormat + " not recognized. Will report in json format.",
          exception);
      formatEnum = KafkaReportingFormats.JSON;
    }

    KafkaReportingFormats eventFormatEnum;
    if (properties.containsKey(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKA_FORMAT)) {
      String eventsReportingFormat = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKA_FORMAT,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_FORMAT);
      try {
        eventFormatEnum = KafkaReportingFormats.valueOf(eventsReportingFormat.toUpperCase());
      } catch (IllegalArgumentException exception) {
        log.warn(
            "Kafka events reporting format " + eventsReportingFormat + " not recognized. Will report in json format.",
            exception);
        eventFormatEnum = KafkaReportingFormats.JSON;
      }
    } else {
      eventFormatEnum = formatEnum;
    }

    if (eventsEnabled) {
      try {
        String eventTopic = eventsTopic.or(defaultTopic).get();
        return eventFormatEnum.buildEventsReporter(brokers, eventTopic, RootMetricContext.get(), properties);
      } catch (IOException exception) {
        throw new MetricReporterException("Failed to create Kafka events reporter.", exception, ReporterType.EVENT, ReporterSinkType.KAFKA);
      }
    }

    return null;
  }
}
