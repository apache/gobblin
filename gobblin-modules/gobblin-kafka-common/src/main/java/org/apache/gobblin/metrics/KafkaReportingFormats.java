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
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventKeyValueReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroMetricKeyValueReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroReporter;
import org.apache.gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import org.apache.gobblin.metrics.kafka.KafkaEventReporter;
import org.apache.gobblin.metrics.kafka.KafkaKeyValueEventObjectReporter;
import org.apache.gobblin.metrics.kafka.KafkaKeyValueMetricObjectReporter;
import org.apache.gobblin.metrics.kafka.KafkaReporter;
import org.apache.gobblin.metrics.kafka.PusherUtils;
import org.apache.gobblin.metrics.reporter.util.KafkaAvroReporterUtil;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Kafka reporting formats enumeration.
 */
public enum KafkaReportingFormats {

  AVRO() {

    @Override
    public void buildMetricsScheduledReporter(String brokers, String topic, Properties properties) throws IOException {

      KafkaAvroReporter.Builder<?> builder = KafkaAvroReporter.BuilderFactory.newBuilder();
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
        builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
      }
      builder.build(brokers, topic, properties);

    }

    @Override
    public ScheduledReporter buildEventsScheduledReporter(String brokers, String topic, MetricContext context, Properties properties) throws IOException {

      KafkaAvroEventReporter.Builder<?> builder = KafkaAvroEventReporter.Factory.forContext(context);
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
        builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
      }
      builder.withConfig(getEventsKafkaConfig(properties));
      String pusherClassName = properties.containsKey(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS)
          ? properties.getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS)
          : properties.getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY,
              PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);
      builder.withPusherClassName(pusherClassName);


      return builder.build(brokers, topic);

    }
  },
  AVRO_KEY_VALUE() {

    @Override
    public void buildMetricsScheduledReporter(String brokers, String topic, Properties properties) throws IOException {

      KafkaAvroMetricKeyValueReporter.Builder<?> builder = KafkaAvroMetricKeyValueReporter.Factory.newBuilder();
      if (properties.containsKey(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS)) {
        List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults()
            .splitToList(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS));
        builder.withKeys(keys);
      }
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
        builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
      }
      builder.build(brokers, topic, properties);

    }

    @Override
    public ScheduledReporter buildEventsScheduledReporter(String brokers, String topic, MetricContext context, Properties properties) throws IOException {

      KafkaAvroEventKeyValueReporter.Builder<?> builder = KafkaAvroEventKeyValueReporter.Factory.forContext(context);
      if (properties.containsKey(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS)) {
        List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults()
            .splitToList(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS));
        builder.withKeys(keys);
      }
      if (Boolean.valueOf(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY,
          ConfigurationKeys.DEFAULT_METRICS_REPORTING_KAFKA_USE_SCHEMA_REGISTRY))) {
        builder.withSchemaRegistry(new KafkaAvroSchemaRegistry(properties));
      }
      builder.withConfig(getEventsKafkaConfig(properties));
      String pusherClassName = properties.containsKey(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS)
          ? properties.getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS)
          : properties.getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY,
              PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);
      builder.withPusherClassName(pusherClassName);

      return builder.build(brokers, topic);

    }
  },
  JSON() {

    @Override
    public void buildMetricsScheduledReporter(String brokers, String topic, Properties properties) throws IOException {
      KafkaReporter.Builder builder = KafkaReporter.BuilderFactory.newBuilder();
      builder.build(brokers, topic, properties);
    }

    @Override
    public ScheduledReporter buildEventsScheduledReporter(String brokers, String topic, MetricContext context, Properties properties) throws IOException {
       KafkaEventReporter.Builder builder = KafkaEventReporter.Factory.forContext(context);
       builder.withConfig(getEventsKafkaConfig(properties));
       String pusherClassName = properties.containsKey(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS)
          ? properties.getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_EVENTS)
          : properties.getProperty(PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY,
              PusherUtils.DEFAULT_KAFKA_PUSHER_CLASS_NAME);
       builder.withPusherClassName(pusherClassName);
       return builder.build(brokers, topic);
    }
  },
  PLAIN_OBJECT() {

    @Override
    public void buildMetricsScheduledReporter(String brokers, String topic, Properties properties) throws IOException {

      KafkaKeyValueMetricObjectReporter.Builder<?> builder = KafkaKeyValueMetricObjectReporter.Factory.newBuilder();
      if (properties.containsKey(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS)) {
        List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults()
            .splitToList(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS));
        builder.withKeys(keys);
      }
      builder.namespaceOverride(KafkaAvroReporterUtil.extractOverrideNamespace(properties));
      builder.build(brokers, topic, properties);

    }

    @Override
    public ScheduledReporter buildEventsScheduledReporter(String brokers, String topic, MetricContext context, Properties properties) throws IOException {

      KafkaKeyValueEventObjectReporter.Builder<?> builder = KafkaKeyValueEventObjectReporter.Factory.forContext(context);
      if (properties.containsKey(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS)) {
        List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults()
            .splitToList(properties.getProperty(ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS));
        builder.withKeys(keys);
      }
      builder.withConfig(getEventsKafkaConfig(properties));

      String objPusherClassKey = PusherUtils.KAFKA_PUSHER_CLASS_NAME_KEY_FOR_OBJECTS;
      String pusherClassName = properties.getProperty(objPusherClassKey, PusherUtils.DEFAULT_KAFKA_KEY_VALUE_PUSHER_CLASS_NAME);
      builder.withPusherClassName(pusherClassName);
      builder.namespaceOverride(KafkaAvroReporterUtil.extractOverrideNamespace(properties));
      return builder.build(brokers, topic);
    }
  };

  public abstract void buildMetricsScheduledReporter(String brokers, String topic, Properties properties) throws IOException;
  public abstract ScheduledReporter buildEventsScheduledReporter(String brokers, String topic, MetricContext context, Properties properties) throws IOException;


  public Config getEventsKafkaConfig(Properties properties){

    Config allConfig = ConfigUtils.propertiesToConfig(properties);
    // the kafka configuration is composed of the metrics reporting specific keys with a fallback to the shared
    // kafka config
    Config kafkaConfig = ConfigUtils.getConfigOrEmpty(allConfig,
        PusherUtils.METRICS_REPORTING_KAFKA_CONFIG_PREFIX).withFallback(ConfigUtils.getConfigOrEmpty(allConfig,
        ConfigurationKeys.SHARED_KAFKA_CONFIG_PREFIX));

    return kafkaConfig;
  }
}
