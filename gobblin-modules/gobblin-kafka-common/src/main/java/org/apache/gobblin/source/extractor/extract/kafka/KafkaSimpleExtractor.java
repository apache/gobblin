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

import java.io.IOException;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;

/**
 * An implementation of {@link KafkaExtractor} from which reads and returns records as an array of bytes.
 *
 * @author akshay@nerdwallet.com
 *
 * @deprecated use {@link KafkaDeserializerExtractor} and {@link KafkaDeserializerExtractor.Deserializers#BYTE_ARRAY} instead
 */
public class KafkaSimpleExtractor extends KafkaExtractor<String, byte[]> {

  private final KafkaSchemaRegistry<String, String> kafkaSchemaRegistry;

  public KafkaSimpleExtractor(WorkUnitState state) {
    super(state);
    this.kafkaSchemaRegistry = new SimpleKafkaSchemaRegistry(state.getProperties());
  }

  @Override
  protected byte[] decodeRecord(ByteArrayBasedKafkaRecord kafkaConsumerRecord) throws IOException {
    return kafkaConsumerRecord.getMessageBytes();
  }

  /**
   * Get the schema (metadata) of the extracted data records.
   *
   * @return the Kafka topic being extracted
   * @throws IOException if there is problem getting the schema
   */
  @Override
  public String getSchema() throws IOException {
    try {
      return this.kafkaSchemaRegistry.getLatestSchemaByTopic(this.topicName);
    } catch (SchemaRegistryException e) {
      throw new RuntimeException(e);
    }
  }
}
