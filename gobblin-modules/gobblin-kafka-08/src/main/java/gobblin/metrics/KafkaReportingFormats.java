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

package gobblin.metrics;

import java.util.Properties;

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
  JSON;

  /**
   * Get a {@link gobblin.metrics.kafka.KafkaReporter.Builder} for this reporting format.
   *
   * @param properties {@link java.util.Properties} containing information to build reporters.
   * @return {@link gobblin.metrics.kafka.KafkaReporter.Builder}.
   */
  public KafkaReporter.Builder<?> metricReporterBuilder(Properties properties) {
    switch (this) {
      case AVRO:
        KafkaAvroReporter.Builder<?> builder = KafkaAvroReporter.BuilderFactory.newBuilder();
        if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
          builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
        }
        return builder;
      case JSON:
        return KafkaReporter.BuilderFactory.newBuilder();
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
        KafkaAvroEventReporter.Builder<?> builder = KafkaAvroEventReporter.Factory.forContext(context);
        if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
            ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
          builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
        }
        return builder;
      case JSON:
        return KafkaEventReporter.Factory.forContext(context);
      default:
        // This should never happen.
        throw new IllegalArgumentException("KafkaReportingFormat not recognized.");
    }
  }
}
