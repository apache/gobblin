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

package gobblin.kafka.serialize;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import gobblin.kafka.schemareg.SchemaRegistryException;


/**
 * LinkedIn's implementation of Avro-schema based serialization for Kafka
 * TODO: Implement this for IndexedRecord not just GenericRecord
 *
 */
public class LiAvroSerializer extends LiAvroSerializerBase implements Serializer<GenericRecord> {

  @Override
  public byte[] serialize(String topic, GenericRecord data) {
    try {
      return super.serialize(topic, data);
    } catch (gobblin.kafka.serialize.SerializationException e) {
      throw new SerializationException(e);
    }
  }
}
