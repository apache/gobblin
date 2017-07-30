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

import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.SchemaRegistryException;


/**
 * LinkedIn's implementation of Avro-schema based serialization for Kafka
 * TODO: Implement this for IndexedRecord not just GenericRecord
 *
 */
public class LiAvroSerializerBase {

  private KafkaSchemaRegistry<MD5Digest, Schema> schemaRegistry;
  private final EncoderFactory encoderFactory;
  private boolean isKey;

  public LiAvroSerializerBase()
  {
    isKey = false;
    encoderFactory = EncoderFactory.get();
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
    if (null == schemaRegistry)
    {
      schemaRegistry = LiAvroSerDeHelper.getSchemaRegistry(configs);
    }
    this.isKey = isKey;
  }

  public byte[] serialize(String topic, GenericRecord data)
      throws SerializationException {
    Schema schema = data.getSchema();
    MD5Digest schemaId = null;
    try {
      schemaId = schemaRegistry.register(topic, schema);
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
    } catch (IOException | SchemaRegistryException e) {
      throw new SerializationException(e);
    }
  }

  public void close() {
  }
}
