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
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.SchemaRegistryException;


/**
 * The LinkedIn Avro Deserializer (works with records serialized by the {@link LiAvroSerializer})
 */
@Slf4j
public class LiAvroDeserializer implements Deserializer<GenericRecord> {

  private KafkaSchemaRegistry<MD5Digest, Schema> _schemaRegistry;
  private GenericDatumReader<GenericData.Record> _datumReader;

  public LiAvroDeserializer(KafkaSchemaRegistry<MD5Digest, Schema> schemaRegistry)
  {
    _schemaRegistry = schemaRegistry;
    _datumReader = new GenericDatumReader<>();
  }
  /**
   * TODO: Not implemented.
   * Configure this class.

   * @param configs configs in key/value pairs
   * @param isKey whether is for key or value
   */
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {

  }

  /**
   *
   * @param topic topic associated with the data
   * @param data serialized bytes
   * @return deserialized object
   */
  @Override
  public GenericRecord deserialize(String topic, byte[] data) {
    Preconditions.checkState(_schemaRegistry!=null, "Schema Registry is not initialized");
    Preconditions.checkState(_datumReader!=null, "Datum Reader is not initialized");
    try {
      // MAGIC_BYTE | schemaId-bytes | avro_payload

      if (data[0] != LiAvroSerDeHelper.MAGIC_BYTE) {
        throw new SerializationException(String.format("Unknown magic byte for topic: %s ", topic));
      }
      MD5Digest schemaId = MD5Digest.fromBytes(data, 1  ); // read start after the first byte (magic byte)
      Schema schema = _schemaRegistry.getById(schemaId);
      Decoder decoder = DecoderFactory.get().binaryDecoder(data, 1 + MD5Digest.MD5_BYTES_LENGTH,
          data.length - MD5Digest.MD5_BYTES_LENGTH - 1, null);
      _datumReader.setSchema(schema);
      try {
        GenericRecord record = _datumReader.read(null, decoder);
        return record;
      } catch (IOException e) {
        log.error(String.format("Error during decoding record for topic %s: ", topic));
        throw e;
      }
    } catch (IOException | SchemaRegistryException e) {
      throw new SerializationException("Error during Deserialization", e);
    }
  }

  /**
   * Close this deserializer
   */
  @Override
  public void close() {
  }
}
