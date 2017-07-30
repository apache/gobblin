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

package gobblin.converter;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Preconditions;

import gobblin.configuration.WorkUnitState;
import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistryFactory;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.kafka.serialize.LiAvroDeserializerBase;
import gobblin.kafka.serialize.SerializationException;
import gobblin.source.extractor.extract.kafka.KafkaSource;
import gobblin.util.EmptyIterable;

import lombok.extern.slf4j.Slf4j;


/**
 * Converts LiKafka byte array messages into avro.
 */
@Slf4j
public class LiKafkaByteArrayMsgToAvroConverter<S> extends ToAvroConverterBase<S, byte[]> {
  KafkaSchemaRegistry schemaRegistry;
  LiAvroDeserializerBase deserializer;

  @Override
  public Converter<S, Schema, byte[], GenericRecord> init(WorkUnitState workUnit) {
    this.schemaRegistry = KafkaSchemaRegistryFactory.getSchemaRegistry(workUnit.getProperties());
    this.deserializer = new LiAvroDeserializerBase(this.schemaRegistry);
    return this;
  }

  @Override
  public Schema convertSchema(S schemaIn, WorkUnitState workUnit)
      throws SchemaConversionException {
    Preconditions.checkArgument(workUnit.contains(KafkaSource.TOPIC_NAME), "Must specify topic name.");
    String topic = workUnit.getProp(KafkaSource.TOPIC_NAME);
    try {
      return (Schema) this.schemaRegistry.getLatestSchema(topic);
    } catch (IOException | SchemaRegistryException e) {
      throw new SchemaConversionException(e);
    }
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, byte[] inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      String topic = workUnit.getProp(KafkaSource.TOPIC_NAME);
      GenericRecord record = this.deserializer.deserialize(topic, inputRecord, outputSchema);
      return new SingleRecordIterable<>(record);
    } catch (SerializationException e) {
      log.error("Cannot decode one record.", e);
      return new EmptyIterable<GenericRecord>();
    }
  }
}
