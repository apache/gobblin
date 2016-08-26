/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.kafka.writer;

/**
 * Configuration keys for a KafkaWriter.
 */
public class KafkaWriterConfigurationKeys {
  public static final String KAFKA_TOPIC = "writer.kafka.topic";
  /**
   * Kafka producer configurations will be passed through as is as long as they are prefixed
   * by the PREFIX specified below.
   */
  public static final String KAFKA_PRODUCER_CONFIG_PREFIX = "writer.kafka.producerConfig.";
  static final String KAFKA_WRITER_PRODUCER_CLASS = "writer.kafka.producerClass";
  static final String KEY_SERIALIZER_CONFIG = "key.serializer";
  static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  static final String VALUE_SERIALIZER_CONFIG = "value.serializer";
  static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  static final String COMMIT_TIMEOUT_MILLIS_CONFIG = "commitTimeoutMillis";
  static final long COMMIT_TIMEOUT_MILLIS_DEFAULT = 60000; // 1 minute
  static final String CLIENT_ID_CONFIG = "client.id";
  static final String CLIENT_ID_DEFAULT = "gobblin";
  static final String COMMIT_STEP_WAIT_TIME_CONFIG = "commitStepWaitTimeMillis";
  static final long COMMIT_STEP_WAIT_TIME_DEFAULT = 500; // 500ms
  static final String FAILURE_ALLOWANCE_PCT_CONFIG = "failureAllowancePercentage";
  static final double FAILURE_ALLOWANCE_PCT_DEFAULT = 20.0;
}
