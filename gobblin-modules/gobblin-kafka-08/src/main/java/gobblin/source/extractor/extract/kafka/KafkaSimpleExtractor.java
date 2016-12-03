/*
 * Copyright (C) 2014-2016 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;

import kafka.message.MessageAndOffset;

import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;

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
  protected byte[] decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
    return getBytes(messageAndOffset.message().payload());
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
