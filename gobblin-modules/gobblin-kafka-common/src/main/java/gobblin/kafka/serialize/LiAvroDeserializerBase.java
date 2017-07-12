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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.KafkaSchemaRegistryFactory;
import gobblin.kafka.schemareg.SchemaRegistryException;


/**
 * The LinkedIn Avro Deserializer (works with records serialized by the {@link LiAvroSerializerBase})
 */
@Slf4j
public class LiAvroDeserializerBase {

  private KafkaSchemaRegistry<MD5Digest, Schema> _schemaRegistry;
  private GenericDatumReader<GenericData.Record> _datumReader;

  public LiAvroDeserializerBase()
  {}

  public LiAvroDeserializerBase(KafkaSchemaRegistry<MD5Digest, Schema> schemaRegistry)
  {
    _schemaRegistry = schemaRegistry;
    _datumReader = new GenericDatumReader<>();
    Preconditions.checkState(_schemaRegistry!=null, "Schema Registry is not initialized");
    Preconditions.checkState(_datumReader!=null, "Datum Reader is not initialized");
  }
  /**
   * Configure this class.
   * @param configs configs in key/value pairs
   * @param isKey whether is for key or value
   */
  public void configure(Map<String, ?> configs, boolean isKey) {
    Preconditions.checkArgument(isKey==false, "LiAvroDeserializer only works for value fields");
    _datumReader = new GenericDatumReader<>();
    Properties props = new Properties();
    for (Map.Entry<String, ?> entry: configs.entrySet())
    {
      String value = String.valueOf(entry.getValue());
      props.setProperty(entry.getKey(), value);
    }

    _schemaRegistry = KafkaSchemaRegistryFactory.getSchemaRegistry(props);
  }

  /**
   *
   * @param topic topic associated with the data
   * @param data serialized bytes
   * @param outputSchema the schema to deserialize to. If null then the record schema is used.
   * @return deserialized object
   */
  public GenericRecord deserialize(String topic, byte[] data, Schema outputSchema)
      throws SerializationException {
    try {
      // MAGIC_BYTE | schemaId-bytes | avro_payload

      if (data[0] != LiAvroSerDeHelper.MAGIC_BYTE) {
        throw new SerializationException(String.format("Unknown magic byte for topic: %s ", topic));
      }
      MD5Digest schemaId = MD5Digest.fromBytes(data, 1  ); // read start after the first byte (magic byte)
      Schema schema = _schemaRegistry.getById(schemaId);
      Decoder decoder = DecoderFactory.get().binaryDecoder(data, 1 + MD5Digest.MD5_BYTES_LENGTH,
          data.length - MD5Digest.MD5_BYTES_LENGTH - 1, null);
      _datumReader.setExpected(outputSchema);
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
   *
   * @param topic topic associated with the data
   * @param data serialized bytes
   * @return deserialized object
   */
  public GenericRecord deserialize(String topic, byte[] data)
          throws SerializationException {
    return deserialize(topic, data, null);
  }

  public void close() {
  }

}
