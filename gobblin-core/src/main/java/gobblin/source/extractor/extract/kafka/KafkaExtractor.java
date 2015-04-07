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
import java.util.Iterator;

import kafka.message.MessageAndOffset;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.extract.EventBasedExtractor;


public class KafkaExtractor extends EventBasedExtractor<Schema, GenericRecord> {

  public static final String KAFKA_SCHEMA_REGISTRY_URL = "kafka.schema.registry.url";
  public static final int SCHEMA_ID_LENGTH_BYTE = 16;
  private static final byte MAGIC_BYTE = 0x0;

  private final Schema schema;
  private final KafkaPartition partition;
  private final long lowWatermark;
  private final long highWatermark;
  private final Closer closer;
  private final KafkaWrapper kafkaWrapper;
  private final KafkaAvroSchemaRegistry schemaRegistry;

  private Iterator<MessageAndOffset> messageIterator;
  private long nextWatermark;

  public KafkaExtractor(WorkUnitState state) {
    this.partition =
        new KafkaPartition.Builder().withId(state.getPropAsInt(KafkaSource.PARTITION_ID))
            .withTopicName(state.getProp(KafkaSource.TOPIC_NAME))
            .withLeaderId(state.getPropAsInt(KafkaSource.LEADER_ID))
            .withLeaderHost(state.getProp(KafkaSource.LEADER_HOST))
            .withLeaderPort(state.getPropAsInt(KafkaSource.LEADER_PORT)).build();
    this.lowWatermark = state.getPropAsLong(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY);
    this.highWatermark = state.getPropAsLong(ConfigurationKeys.WORK_UNIT_HIGH_WATER_MARK_KEY);
    this.closer = Closer.create();
    this.kafkaWrapper = closer.register(KafkaWrapper.create(state));
    this.schemaRegistry = new KafkaAvroSchemaRegistry(state.getProp(KAFKA_SCHEMA_REGISTRY_URL));
    this.schema = getLatestSchemaByTopic();
    this.messageIterator = null;
    this.nextWatermark = this.lowWatermark;
  }

  private Schema getLatestSchemaByTopic() {
    try {
      return this.schemaRegistry.getLatestSchemaByTopic(this.partition.getTopicName());
    } catch (SchemaNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public GenericRecord readRecord(GenericRecord reuse) throws DataRecordException, IOException {
    if (this.nextWatermark > this.highWatermark) {
      return null;
    }
    if (this.messageIterator == null || !this.messageIterator.hasNext()) {
      this.messageIterator =
          this.kafkaWrapper.fetchNextMessageBuffer(this.partition, this.nextWatermark, this.highWatermark);
      if (this.messageIterator == null || !this.messageIterator.hasNext()) {
        return null;
      }
    }

    MessageAndOffset nextValidMessage = null;
    do {
      if (!this.messageIterator.hasNext()) {
        return null;
      }
      nextValidMessage = this.messageIterator.next();
    } while (nextValidMessage.offset() < this.nextWatermark);

    this.nextWatermark = nextValidMessage.offset() + 1;
    return decodeRecord(nextValidMessage);
  }

  private GenericRecord decodeRecord(MessageAndOffset messageAndOffset) {
    byte[] payload = getBytes(messageAndOffset.message().payload());
    if (payload[0] != MAGIC_BYTE) {
      throw new RuntimeException(String.format("Unknown magic byte for topic %s, partition %d",
          this.partition.getTopicName(), this.partition.getId()));
    }

    byte[] schemaIdByteArray = new byte[16];
    schemaIdByteArray = Arrays.copyOfRange(payload, 1, 1 + SCHEMA_ID_LENGTH_BYTE);
    String schemaId = byteArrayToHexString(schemaIdByteArray);

    Schema schema = null;
    try {
      schema = this.schemaRegistry.getSchemaById(schemaId);
    } catch (SchemaNotFoundException e) {
      throw new RuntimeException(e);
    }
    DatumReader<Record> reader = new GenericDatumReader<Record>(schema);
    Decoder binaryDecoder =
        DecoderFactory.get().binaryDecoder(payload, 1 + SCHEMA_ID_LENGTH_BYTE,
            payload.length - 1 - SCHEMA_ID_LENGTH_BYTE, null);
    try {
      return reader.read(null, binaryDecoder);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Error during decoding record for topic %s, partition %d: ",
          this.partition.getTopicName(), this.partition.getId()), e);
    }
  }

  public static String byteArrayToHexString(byte[] bytes) {
    StringBuilder builder = new StringBuilder(2 * bytes.length);
    for (int i = 0; i < bytes.length; i++) {
      String hexString = Integer.toHexString(0xFF & bytes[i]);
      if (hexString.length() < 2)
        hexString = "0" + hexString;
      builder.append(hexString);
    }
    return builder.toString();
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

  @Override
  public long getExpectedRecordCount() {
    return this.highWatermark - this.lowWatermark;
  }

  @Override
  public long getHighWatermark() {
    return this.highWatermark;
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

}
