/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
import java.util.Arrays;

import kafka.message.MessageAndOffset;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaAvroSchemaRegistry;
import gobblin.metrics.kafka.SchemaNotFoundException;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.AvroUtils;


/**
 * An implementation of {@link Extractor} for Kafka, where events are in Avro format.
 *
 * @author ziliu
 */
public class KafkaAvroExtractor extends KafkaExtractor<Schema, GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroExtractor.class);
  private static final Schema DEFAULT_SCHEMA = SchemaBuilder.record("DefaultSchema").fields().name("header")
      .type(SchemaBuilder.record("header").fields().name("time").type("long").withDefault(0).endRecord()).noDefault()
      .endRecord();

  private final Optional<Schema> schema;
  private final KafkaAvroSchemaRegistry schemaRegistry;
  private final Optional<GenericDatumReader<Record>> reader;

  /**
   * @param state state should contain property "kafka.schema.registry.url", and optionally
   * "kafka.schema.registry.max.cache.size" (default = 1000) and
   * "kafka.schema.registry.cache.expire.after.write.min" (default = 10).
   * @throws SchemaNotFoundException if the latest schema of the topic cannot be retrieved
   * from the schema registry.
   */
  public KafkaAvroExtractor(WorkUnitState state) {
    super(state);
    this.schemaRegistry = new KafkaAvroSchemaRegistry(state.getProperties());
    this.schema = Optional.fromNullable(getLatestSchemaByTopic());
    if (this.schema.isPresent()) {
      this.reader = Optional.of(new GenericDatumReader<Record>(this.schema.get()));
    } else {
      this.reader = Optional.absent();
    }
  }

  private Schema getLatestSchemaByTopic() {
    try {
      return this.schemaRegistry.getLatestSchemaByTopic(this.topicName);
    } catch (SchemaNotFoundException e) {
      LOG.error(String.format("Cannot find latest schema for topic %s. This topic will be skipped", this.topicName), e);
      return null;
    }
  }

  @Override
  public GenericRecord readRecordImpl(GenericRecord reuse) throws DataRecordException, IOException {
    if (!this.schema.isPresent()) {
      return null;
    }
    return super.readRecordImpl(reuse);
  }

  @Override
  public Schema getSchema() {
    return this.schema.or(DEFAULT_SCHEMA);
  }

  @Override
  protected GenericRecord decodeRecord(MessageAndOffset messageAndOffset) throws SchemaNotFoundException, IOException {
    byte[] payload = getBytes(messageAndOffset.message().payload());
    if (payload[0] != KafkaAvroSchemaRegistry.MAGIC_BYTE) {
      throw new RuntimeException(String.format("Unknown magic byte for partition %s", this.getCurrentPartition()));
    }

    byte[] schemaIdByteArray = Arrays.copyOfRange(payload, 1, 1 + KafkaAvroSchemaRegistry.SCHEMA_ID_LENGTH_BYTE);
    String schemaId = Hex.encodeHexString(schemaIdByteArray);
    Schema schema = null;
    schema = this.schemaRegistry.getSchemaById(schemaId);
    reader.get().setSchema(schema);
    Decoder binaryDecoder =
        DecoderFactory.get().binaryDecoder(payload, 1 + KafkaAvroSchemaRegistry.SCHEMA_ID_LENGTH_BYTE,
            payload.length - 1 - KafkaAvroSchemaRegistry.SCHEMA_ID_LENGTH_BYTE, null);
    try {
      GenericRecord record = reader.get().read(null, binaryDecoder);
      record = AvroUtils.convertRecordSchema(record, this.schema.get());
      return record;
    } catch (IOException e) {
      LOG.error(String.format("Error during decoding record for partition %s: ", this.getCurrentPartition()));
      throw e;
    }
  }
}
