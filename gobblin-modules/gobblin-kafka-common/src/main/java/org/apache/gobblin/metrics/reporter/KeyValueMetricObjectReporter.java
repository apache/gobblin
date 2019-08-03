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

package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.MetricReport;
import org.apache.gobblin.metrics.kafka.PusherUtils;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ConfigUtils;


/**
 * This is a raw metric (MetricReport) key value reporter that reports metrics as GenericRecords without serialization
 * Configuration for this reporter start with the prefix "metrics.reporting"
 */
@Slf4j
public class KeyValueMetricObjectReporter extends MetricReportReporter {

  private static final String PUSHER_CONFIG = "pusherConfig";
  private static final String PUSHER_CLASS = "pusherClass";
  private static final String PUSHER_KEYS = "pusherKeys";
  private static final String KEY_DELIMITER = ",";
  private static final String KEY_SIZE_KEY = "keySize";

  private List<String> keys;
  protected final String randomKey;
  protected KeyValuePusher pusher;
  protected final Schema schema;

  public KeyValueMetricObjectReporter(Builder builder, Config config) {
    super(builder, config);

    Config pusherConfig = ConfigUtils.getConfigOrEmpty(config, PUSHER_CONFIG).withFallback(config);
    String pusherClassName =
        ConfigUtils.getString(config, PUSHER_CLASS, PusherUtils.DEFAULT_KEY_VALUE_PUSHER_CLASS_NAME);
    this.pusher = (KeyValuePusher) PusherUtils
        .getPusher(pusherClassName, builder.brokers, builder.topic, Optional.of(pusherConfig));
    this.closer.register(this.pusher);

    randomKey = String.valueOf(
        new Random().nextInt(ConfigUtils.getInt(config, KEY_SIZE_KEY, ConfigurationKeys.DEFAULT_REPORTER_KEY_SIZE)));
    if (config.hasPath(PUSHER_KEYS)) {
      List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(config.getString(PUSHER_KEYS));
      this.keys = keys;
    } else {
      log.info(
          "Key not assigned from config. Please set it with property {} Using randomly generated number {} as key ",
          ConfigurationKeys.METRICS_REPORTING_PUSHERKEYS, randomKey);
    }

    schema = AvroUtils.overrideNameAndNamespace(MetricReport.getClassSchema(), builder.topic, builder.namespaceOverride);
  }

  @Override
  protected void emitReport(MetricReport report) {
    GenericRecord record = report;
    try {
      record = AvroUtils.convertRecordSchema(report, schema);
    } catch (IOException e){
      log.error("Unable to generate generic data record", e);
      return;
    }
    this.pusher.pushKeyValueMessages(Lists.newArrayList(Pair.of(buildKey(record), record)));
  }

  private String buildKey(GenericRecord report) {

    String key = randomKey;
    if (this.keys != null && this.keys.size() > 0) {

      StringJoiner joiner = new StringJoiner(KEY_DELIMITER);
      for (String keyPart : keys) {
        Optional value = AvroUtils.getFieldValue(report, keyPart);
        if (value.isPresent()) {
          joiner.add(value.get().toString());
        } else {
          log.error("{} not found in the MetricReport. Setting key to {}", keyPart, key);
          return key;
        }
      }

      key = joiner.toString();
    }

    return key;
  }

  public static class Builder extends MetricReportReporter.Builder<Builder> {
    protected String brokers;
    protected String topic;
    protected Optional<Map<String, String>> namespaceOverride = Optional.absent();

    public Builder namespaceOverride(Optional<Map<String, String>> namespaceOverride) {
      this.namespaceOverride = namespaceOverride;
      return self();
    }

    public KeyValueMetricObjectReporter build(String brokers, String topic, Config config)
        throws IOException {
      this.brokers = brokers;
      this.topic = topic;
      return new KeyValueMetricObjectReporter(this, config);
    }

    @Override
    protected Builder self() {
      return this;
    }
  }
}
