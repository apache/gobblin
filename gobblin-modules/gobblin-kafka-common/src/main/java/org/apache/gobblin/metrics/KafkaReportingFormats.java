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

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import com.codahale.metrics.ScheduledReporter;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventKeyValueReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaEventReporter;
import org.apache.gobblin.metrics.reporter.KeyValueEventObjectReporter;
import org.apache.gobblin.metrics.reporter.KeyValueMetricObjectReporter;
import org.apache.gobblin.metrics.kafka.KafkaReporter;
import org.apache.gobblin.metrics.kafka.PusherUtils;
import org.apache.gobblin.metrics.reporter.util.KafkaReporterUtils;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Kafka reporting formats enumeration.
 */
public enum KafkaReportingFormats {

  AVRO() {
    @Override
    public void buildMetricsReporter(String brokers, String topic, Properties properties)
        throws IOException {

      KafkaAvroReporter.Builder<?> builder = KafkaAvroReporter.BuilderFactory.newBuilder();
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
        builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
        String schemaId = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_METRICS_KAFKA_AVRO_SCHEMA_ID);
        if (!Strings.isNullOrEmpty(schemaId)) {
          builder.withSchemaId(schemaId);
        }
      }
      builder.build(brokers, topic, properties);
    }

    @Override
    public ScheduledReporter buildEventsReporter(String brokers, String topic, MetricContext context,
        Properties properties)
        throws IOException {

      KafkaAvroEventReporter.Builder<?> builder = KafkaAvroEventReporter.Factory.forContext(context);
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
        builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
        String schemaId = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKA_AVRO_SCHEMA_ID);
        if (!Strings.isNullOrEmpty(schemaId)) {
          builder.withSchemaId(schemaId);
        }
      }
      String pusherClassName = properties.containsKey(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS) ? properties
          .getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS) : properties
          .getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY, PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);
      builder.withPusherClassName(pusherClassName);

      Config allConfig = ConfigUtils.propertiesToConfig(properties);
      builder.withConfig(allConfig);
      // the kafka configuration is composed of the metrics reporting specific keys with a fallback to the shared
      // kafka config
      Config kafkaConfig = ConfigUtils.getConfigOrEmpty(allConfig, PusherUtils.METRICS_REPORTING_KAFKA_CONFIG_PREFIX)
          .withFallback(ConfigUtils.getConfigOrEmpty(allConfig, ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX));

      builder.withKafkaConfig(kafkaConfig);

      return builder.build(brokers, topic);
    }
  }, AVRO_KEY_VALUE() {
    @Override
    public void buildMetricsReporter(String brokers, String topic, Properties properties)
        throws IOException {

      throw new IOException("Unsupported format for Metric reporting " + this.name());
    }

    @Override
    public ScheduledReporter buildEventsReporter(String brokers, String topic, MetricContext context,
        Properties properties)
        throws IOException {

      KafkaAvroEventKeyValueReporter.Builder<?> builder = KafkaAvroEventKeyValueReporter.Factory.forContext(context);
      if (properties.containsKey(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS)) {
        List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults()
            .splitToList(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS));
        builder.withKeys(keys);
      }
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
        builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
        String schemaId = properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKA_AVRO_SCHEMA_ID);
        if (!Strings.isNullOrEmpty(schemaId)) {
          builder.withSchemaId(schemaId);
        }
      }
      String pusherClassName = properties.containsKey(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS) ? properties
          .getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS) : properties
          .getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY, PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);
      builder.withPusherClassName(pusherClassName);

      Config allConfig = ConfigUtils.propertiesToConfig(properties);
      builder.withConfig(allConfig);
      // the kafka configuration is composed of the metrics reporting specific keys with a fallback to the shared
      // kafka config
      Config kafkaConfig = ConfigUtils.getConfigOrEmpty(allConfig, PusherUtils.METRICS_REPORTING_KAFKA_CONFIG_PREFIX)
          .withFallback(ConfigUtils.getConfigOrEmpty(allConfig, ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX));

      builder.withKafkaConfig(kafkaConfig);

      return builder.build(brokers, topic);
    }
  }, JSON() {
    @Override
    public void buildMetricsReporter(String brokers, String topic, Properties properties)
        throws IOException {
      KafkaReporter.Builder builder = KafkaReporter.BuilderFactory.newBuilder();
      builder.build(brokers, topic, properties);
    }

    @Override
    public ScheduledReporter buildEventsReporter(String brokers, String topic, MetricContext context,
        Properties properties)
        throws IOException {
      KafkaEventReporter.Builder builder = KafkaEventReporter.Factory.forContext(context);
      //builder.withConfig(getEventsConfig(properties));
      String pusherClassName = properties.containsKey(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS) ? properties
          .getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS) : properties
          .getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY, PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);
      builder.withPusherClassName(pusherClassName);

      Config allConfig = ConfigUtils.propertiesToConfig(properties);
      builder.withConfig(allConfig);
      // the kafka configuration is composed of the metrics reporting specific keys with a fallback to the shared
      // kafka config
      Config kafkaConfig = ConfigUtils.getConfigOrEmpty(allConfig, PusherUtils.METRICS_REPORTING_KAFKA_CONFIG_PREFIX)
          .withFallback(ConfigUtils.getConfigOrEmpty(allConfig, ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX));

      builder.withKafkaConfig(kafkaConfig);

      return builder.build(brokers, topic);
    }
  }, PLAIN_OBJECT() {
    @Override
    public void buildMetricsReporter(String brokers, String topic, Properties properties)
        throws IOException {

      KeyValueMetricObjectReporter.Builder builder = new KeyValueMetricObjectReporter.Builder();
      builder.namespaceOverride(KafkaReporterUtils.extractOverrideNamespace(properties));
      Config allConfig = ConfigUtils.propertiesToConfig(properties);
      Config config = ConfigUtils.getConfigOrEmpty(allConfig, ConfigurationKeys.METRICS_REPORTING_CONFIGURATIONS_PREFIX)
          .withFallback(allConfig);
      builder.build(brokers, topic, config);
    }

    @Override
    public ScheduledReporter buildEventsReporter(String brokers, String topic, MetricContext context,
        Properties properties)
        throws IOException {

      KeyValueEventObjectReporter.Builder builder = new KeyValueEventObjectReporter.Builder(context);
      Config allConfig = ConfigUtils.propertiesToConfig(properties);
      Config config =
          ConfigUtils.getConfigOrEmpty(allConfig, ConfigurationKeys.METRICS_REPORTING_EVENTS_CONFIGURATIONS_PREFIX)
              .withFallback(allConfig);
      builder.withConfig(config);
      builder.namespaceOverride(KafkaReporterUtils.extractOverrideNamespace(properties));
      return builder.build(brokers, topic);
    }
  };

  /**
   * Method to build reporters that emit metrics. This method does not return anything but schedules/starts the reporter internally
   * @param brokers Kafka broker to connect
   * @param topic Kafka topic to publish data
   * @param properties Properties to build configurations from
   * @throws IOException
   */
  public abstract void buildMetricsReporter(String brokers, String topic, Properties properties)
      throws IOException;

  /**
   * Method to build reporters that emit events.
   * @param brokers Kafka broker to connect
   * @param topic Kafka topic to publish data
   * @param context MetricContext to report
   * @param properties Properties to build configurations from
   * @return an instance of the event reporter
   * @throws IOException
   */
  public abstract ScheduledReporter buildEventsReporter(String brokers, String topic, MetricContext context,
      Properties properties)
      throws IOException;

}
