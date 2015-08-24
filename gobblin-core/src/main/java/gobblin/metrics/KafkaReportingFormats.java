/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metrics;

import java.util.Properties;

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.kafka.KafkaAvroEventReporter;
import gobblin.metrics.kafka.KafkaAvroReporter;
import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.KafkaEventReporter;
import gobblin.metrics.kafka.KafkaReporter;


/**
 * Kafka reporting formats enumeration.
 */
public enum KafkaReportingFormats {

  AVRO,
  JSON,
  CLASS;

  /**
   * Get a {@link gobblin.metrics.kafka.KafkaReporter.Builder} for this reporting format.
   * @param context {@link gobblin.metrics.MetricContext} that should be reported.
   * @param properties {@link java.util.Properties} containing information to build reporters.
   * @return {@link gobblin.metrics.kafka.KafkaReporter.Builder}.
   */
  public KafkaReporter.Builder<?> metricReporterBuilder(MetricContext context, Properties properties) {
    switch (this) {
      case AVRO:
        KafkaAvroReporter.Builder<?> avroBuilder = KafkaAvroReporter.forContext(context);
        if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
          avroBuilder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
        }
        return avroBuilder;
      case JSON:
        return KafkaReporter.forContext(context);
      case CLASS:
        Preconditions.checkArgument(properties.containsKey(ConfigurationKeys.METRICS_KAFKA_REPORTER_BUILDER_CLASS),
            "Missing required property " + ConfigurationKeys.METRICS_KAFKA_REPORTER_BUILDER_CLASS);
        try {
          KafkaReporter.Builder<?> classBuilder = (KafkaReporter.Builder<?>) Class
              .forName(properties.getProperty(ConfigurationKeys.METRICS_KAFKA_REPORTER_BUILDER_CLASS))
              .getMethod("forContext", MetricContext.class).invoke(null, context);
          if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
              ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
            classBuilder.getClass().getMethod("withSchemaRegistry", KafkaAvroSchemaRegistry.class).invoke(classBuilder,
                new KafkaAvroSchemaRegistry(properties));
          }
        } catch (Exception e) {
          throw new RuntimeException("Unable to create KafkaReporter.Builder from "
              + properties.getProperty(ConfigurationKeys.METRICS_KAFKA_REPORTER_BUILDER_CLASS));
        }
      default:
        // This should never happen.
        throw new IllegalArgumentException("KafkaReportingFormat not recognized.");
    }
  }

  /**
   * Get a {@link gobblin.metrics.kafka.KafkaEventReporter.Builder} for this reporting format.
   * @param context {@link gobblin.metrics.MetricContext} that should be reported.
   * @param properties {@link java.util.Properties} containing information to build reporters.
   * @return {@link gobblin.metrics.kafka.KafkaEventReporter.Builder}.
   */
  public KafkaEventReporter.Builder<?> eventReporterBuilder(MetricContext context, Properties properties) {
    switch (this) {
      case AVRO:
        KafkaAvroEventReporter.Builder<?> builder = KafkaAvroEventReporter.forContext(context);
        if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
          builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
        }
        return builder;
      case JSON:
        return KafkaEventReporter.forContext(context);
      default:
        // This should never happen.
        throw new IllegalArgumentException("KafkaReportingFormat not recognized.");
    }
  }

}
