/* (c) 2015 LinkedIn Corp. All rights reserved.
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
import java.nio.ByteBuffer;
import java.util.Arrays;

import kafka.message.MessageAndOffset;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.util.AvroUtils;


/**
 * An implementation of {@link Extractor} for Kafka, where events are in Avro format.
 *
 * @author ziliu
 */
public class KafkaAvroExtractor extends KafkaExtractor<Schema, GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroExtractor.class);

  public static final int SCHEMA_ID_LENGTH_BYTE = 16;
  private static final byte MAGIC_BYTE = 0x0;

  private final Schema schema;
  private final KafkaAvroSchemaRegistry schemaRegistry;
  private final DatumReader<Record> reader;

  /**
   * @param state state should contain property "kafka.schema.registry.url", and optionally
   * "kafka.schema.registry.max.cache.size" (default = 1000) and
   * "kafka.schema.registry.cache.expire.after.write.min" (default = 10).
   * @throws SchemaNotFoundException if the latest schema of the topic cannot be retrieved
   * from the schema registry.
   */
  public KafkaAvroExtractor(WorkUnitState state) throws SchemaNotFoundException {
    super(state);
    this.schemaRegistry = new KafkaAvroSchemaRegistry(state);
    this.schema = getLatestSchemaByTopic();
    this.reader = new GenericDatumReader<Record>(this.schema);
  }

  private Schema getLatestSchemaByTopic() throws SchemaNotFoundException {
    return this.schemaRegistry.getLatestSchemaByTopic(this.partition.getTopicName());
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  protected GenericRecord decodeRecord(MessageAndOffset messageAndOffset, GenericRecord reuse)
      throws SchemaNotFoundException, IOException {
    byte[] payload = getBytes(messageAndOffset.message().payload());
    if (payload[0] != MAGIC_BYTE) {
      throw new RuntimeException(String.format("Unknown magic byte for topic %s, partition %d",
          this.partition.getTopicName(), this.partition.getId()));
    }

    byte[] schemaIdByteArray = Arrays.copyOfRange(payload, 1, 1 + SCHEMA_ID_LENGTH_BYTE);
    String schemaId = Hex.encodeHexString(schemaIdByteArray);
    Schema schema = null;
    schema = this.schemaRegistry.getSchemaById(schemaId);
    reader.setSchema(schema);
    Decoder binaryDecoder =
        DecoderFactory.get().binaryDecoder(payload, 1 + SCHEMA_ID_LENGTH_BYTE,
            payload.length - 1 - SCHEMA_ID_LENGTH_BYTE, null);
    try {
      GenericRecord record = reader.read((GenericData.Record) reuse, binaryDecoder);
      if (!record.getSchema().equals(this.schema)) {
        record = AvroUtils.convertRecordSchema(record, this.schema);
      }
      return record;
    } catch (IOException e) {
      LOG.error(String.format("Error during decoding record for topic %s, partition %d: ",
          this.partition.getTopicName(), this.partition.getId()));
      throw e;
    }
  }

  private static byte[] getBytes(ByteBuffer buf) {
    byte[] bytes = null;
    if (buf != null) {
      int size = buf.remaining();
      bytes = new byte[size];
      buf.get(bytes, buf.position(), size);
    }
    return bytes;
  }

}
