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

package org.apache.gobblin.kafka.writer;

import java.util.Properties;

import org.apache.gobblin.kafka.serialize.GsonSerializerBase;
import org.apache.gobblin.writer.AsyncDataWriter;
import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


/**
 * A {@link org.apache.gobblin.writer.DataWriterBuilder} that builds a {@link org.apache.gobblin.writer.DataWriter} to
 * write {@link JsonObject} to kafka
 */
public class Kafka09JsonObjectWriterBuilder extends AbstractKafkaDataWriterBuilder<JsonArray, JsonObject> {
  private static final String VALUE_SERIALIZER_KEY =
      KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + KafkaWriterConfigurationKeys.VALUE_SERIALIZER_CONFIG;

  @Override
  protected AsyncDataWriter<JsonObject> getAsyncDataWriter(Properties props) {
    props.setProperty(VALUE_SERIALIZER_KEY, KafkaGsonObjectSerializer.class.getName());
    return new Kafka09DataWriter<>(props);
  }

  /**
   * A specific {@link Serializer} that serializes {@link JsonObject} to byte array
   */
  public final static class KafkaGsonObjectSerializer extends GsonSerializerBase<JsonObject> implements Serializer<JsonObject> {
  }
}
