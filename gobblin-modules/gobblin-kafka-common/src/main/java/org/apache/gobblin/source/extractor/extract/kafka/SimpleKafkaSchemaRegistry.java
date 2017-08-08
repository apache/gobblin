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

package org.apache.gobblin.source.extractor.extract.kafka;

import java.util.Properties;

import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;


/**
 * Extension of {@link KafkaSchemaRegistry} that treats the topic name and the schema as the same string. The
 * {@link #getLatestSchemaByTopic(String)} topic will simplye return the specified topic name. All other methods throw
 * an {@link UnsupportedOperationException}. This class is useful when Kafka records don't have a schema, for example,
 * in {@link KafkaSimpleExtractor} or {@link KafkaGsonDeserializer}.
 */
public class SimpleKafkaSchemaRegistry extends KafkaSchemaRegistry<String, String> {

  public SimpleKafkaSchemaRegistry(Properties props) {
    super(props);
  }

  @Override
  protected String fetchSchemaByKey(String key) throws SchemaRegistryException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getLatestSchemaByTopic(String topic) throws SchemaRegistryException {
    return topic;
  }

  @Override
  public String register(String schema) throws SchemaRegistryException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String register(String schema, String name) throws SchemaRegistryException {
    throw new UnsupportedOperationException();
  }
}
