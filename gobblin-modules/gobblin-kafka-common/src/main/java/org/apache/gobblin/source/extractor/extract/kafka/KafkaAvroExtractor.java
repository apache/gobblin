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

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.WorkUnitState;
import gobblin.kafka.client.ByteArrayBasedKafkaRecord;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.AvroUtils;


/**
 * An abstract implementation of {@link Extractor} for Kafka, where events are in Avro format.
 *
 * Subclasses should implement {@link #getRecordSchema(byte[])} and {@link #getDecoder(byte[])}. Additionally, if
 * schema registry is not used (i.e., property {@link KafkaSchemaRegistry#KAFKA_SCHEMA_REGISTRY_CLASS} is not
 * specified, method {@link #getExtractorSchema()} should be overriden.
 *
 * @author Ziyang Liu
 */
@Slf4j
public abstract class KafkaAvroExtractor<K> extends KafkaExtractor<Schema, GenericRecord> {

  protected static final Schema DEFAULT_SCHEMA = SchemaBuilder.record("DefaultSchema").fields().name("header")
      .type(SchemaBuilder.record("header").fields().name("time").type("long").withDefault(0).endRecord()).noDefault()
      .endRecord();

  protected final Optional<KafkaSchemaRegistry<K, Schema>> schemaRegistry;
  protected final Optional<Schema> schema;
  protected final Optional<GenericDatumReader<Record>> reader;

  public KafkaAvroExtractor(WorkUnitState state) {
    super(state);
    this.schemaRegistry = state.contains(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS)
        ? Optional.of(KafkaSchemaRegistry.<K, Schema> get(state.getProperties()))
        : Optional.<KafkaSchemaRegistry<K, Schema>> absent();
    this.schema = getExtractorSchema();
    if (this.schema.isPresent()) {
      this.reader = Optional.of(new GenericDatumReader<Record>(this.schema.get()));
    } else {
      log.error(String.format("Cannot find latest schema for topic %s. This topic will be skipped", this.topicName));
      this.reader = Optional.absent();
    }
  }

  /**
   * Get the schema to be used by this extractor. All extracted records that have different schemas
   * will be converted to this schema.
   */
  protected Optional<Schema> getExtractorSchema() {
    return Optional.fromNullable(getLatestSchemaByTopic(this.topicName));
  }

  protected Schema getLatestSchemaByTopic(String topic) {
    Preconditions.checkState(this.schemaRegistry.isPresent());
    try {
      return this.schemaRegistry.get().getLatestSchemaByTopic(topic);
    } catch (SchemaRegistryException e) {
      log.error(String.format("Cannot find latest schema for topic %s. This topic will be skipped", topic), e);
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
  protected GenericRecord decodeRecord(ByteArrayBasedKafkaRecord messageAndOffset) throws IOException {
    byte[] payload = messageAndOffset.getMessageBytes();
    Schema recordSchema = getRecordSchema(payload);
    Decoder decoder = getDecoder(payload);
    this.reader.get().setSchema(recordSchema);
    try {
      GenericRecord record = this.reader.get().read(null, decoder);
      record = convertRecord(record);
      return record;
    } catch (IOException e) {
      log.error(String.format("Error during decoding record for partition %s: ", this.getCurrentPartition()));
      throw e;
    }
  }

  /**
   * Convert the record to the output schema of this extractor
   * @param record the input record
   * @return the converted record
   * @throws IOException
   */
  @Override
  protected GenericRecord convertRecord(GenericRecord record) throws IOException {
    return AvroUtils.convertRecordSchema(record, this.schema.get());
  }

  /**
   * Obtain the Avro {@link Schema} of a Kafka record given the payload of the record.
   */
  protected abstract Schema getRecordSchema(byte[] payload);

  /**
   * Obtain the Avro {@link Decoder} for a Kafka record given the payload of the record.
   */
  protected abstract Decoder getDecoder(byte[] payload);
}
