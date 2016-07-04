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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.SchemaRegistryException;


/**
 * LinkedIn's implementation of Avro-schema based serialization for Kafka
 * TODO: Implement this for IndexedRecord not just GenericRecord
 *
 */
public class LiAvroSerializer implements Serializer<GenericRecord> {

  private KafkaSchemaRegistry<MD5Digest, Schema> schemaRegistry;
  private final EncoderFactory encoderFactory;
  private boolean isKey;

  public LiAvroSerializer()
  {
    isKey = false;
    encoderFactory = EncoderFactory.get();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    if (null == schemaRegistry)
    {
      schemaRegistry = LiAvroSerDeHelper.getSchemaRegistry(configs);
    }
    this.isKey = isKey;
  }

  @Override
  public byte[] serialize(String topic, GenericRecord data) {
    Schema schema = data.getSchema();
    try {
      MD5Digest schemaId = schemaRegistry.register(topic, schema);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      // MAGIC_BYTE | schemaId-bytes | avro_payload
      out.write(LiAvroSerDeHelper.MAGIC_BYTE);
      out.write(schemaId.asBytes());
      BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
      DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
      writer.write(data, encoder);
      encoder.flush();
      byte[] bytes = out.toByteArray();
      out.close();
      return bytes;
    } catch (IOException | IllegalArgumentException | SchemaRegistryException e) {
      throw new SerializationException(e);
    }
  }

  @Override
  public void close() {

  }
}
