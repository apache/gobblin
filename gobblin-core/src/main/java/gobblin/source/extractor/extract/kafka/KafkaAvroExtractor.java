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

import kafka.message.MessageAndOffset;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.AvroUtils;


/**
 * An abstract implementation of {@link Extractor} for Kafka, where events are in Avro format.
 *
 * @author ziliu
 */
public abstract class KafkaAvroExtractor extends KafkaExtractor<Schema, GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroExtractor.class);
  private static final Schema DEFAULT_SCHEMA = SchemaBuilder.record("DefaultSchema").fields().name("header")
      .type(SchemaBuilder.record("header").fields().name("time").type("long").withDefault(0).endRecord()).noDefault()
      .endRecord();

  private final Optional<Schema> schema;
  private final Optional<GenericDatumReader<Record>> reader;

  public KafkaAvroExtractor(WorkUnitState state) {
    super(state);
    this.schema = getExtractorSchema();
    if (this.schema.isPresent()) {
      this.reader = Optional.of(new GenericDatumReader<Record>(this.schema.get()));
    } else {
      LOG.error(String.format("Cannot find latest schema for topic %s. This topic will be skipped", this.topicName));
      this.reader = Optional.absent();
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
  protected GenericRecord decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
    byte[] payload = getBytes(messageAndOffset.message().payload());
    Schema recordSchema = getRecordSchema(payload);
    Decoder decoder = getDecoder(payload);
    this.reader.get().setSchema(recordSchema);
    try {
      GenericRecord record = this.reader.get().read(null, decoder);
      record = AvroUtils.convertRecordSchema(record, this.schema.get());
      return record;
    } catch (IOException e) {
      LOG.error(String.format("Error during decoding record for partition %s: ", this.getCurrentPartition()));
      throw e;
    }
  }

  /**
   * Get the schema to be used by this extractor. All extracted records that have different schemas
   * will be converted to this schema.
   */
  protected abstract Optional<Schema> getExtractorSchema();

  /**
   * Obtain the {@link Schema} of a Kafka record given the payload of the record.
   */
  protected abstract Schema getRecordSchema(byte[] payload);

  /**
   * Obtain the {@link Decoder} for a Kafka record given the payload of the record.
   */
  protected abstract Decoder getDecoder(byte[] payload);
}
