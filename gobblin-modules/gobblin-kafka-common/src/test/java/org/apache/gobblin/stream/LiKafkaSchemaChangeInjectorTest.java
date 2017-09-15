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
package org.apache.gobblin.stream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.schemareg.ConfigDrivenMd5SchemaRegistry;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.kafka.schemareg.SchemaRegistryException;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;

public class LiKafkaSchemaChangeInjectorTest {
  // Test Avro schema
  private static final String SCHEMA1 = "{\"namespace\": \"example.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"user\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"name\", \"type\": \"string\"},\n" +
      "     {\"name\": \"DUMMY\", \"type\": [\"null\",\"string\"]}\n" +
      " ]\n" +
      "}";

  @Test
  public void testInjection() throws SchemaRegistryException, IOException {
    LiKafkaByteArrayMsgSchemaChangeInjector<Schema> schemaChangeInjector = new LiKafkaByteArrayMsgSchemaChangeInjector();
    WorkUnitState wus = new WorkUnitState();


    wus.setProp(KafkaSource.TOPIC_NAME, "topic1");
    wus.setProp(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());
    schemaChangeInjector.init(wus);
    Schema schema1 = new Schema.Parser().parse(SCHEMA1);
    Schema schema2 = new Schema.Parser().parse(SCHEMA1.replace("DUMMY", "DUMMY2"));
    Schema schema3 = new Schema.Parser().parse(SCHEMA1.replace("DUMMY", "DUMMY3"));
    Schema schema4 = new Schema.Parser().parse(SCHEMA1.replace("DUMMY", "DUMMY4"));

    schemaChangeInjector.setInputGlobalMetadata(GlobalMetadata.<Schema>builder().schema(schema1).build(), null);

    schemaChangeInjector.getSchemaRegistry().register("topic1", schema1);

    byte[] record1 = recordToBytes(getRecord(schema1, "name1"));
    byte[] record2 = recordToBytes(getRecord(schema2, "name1"));
    byte[] record3 = recordToBytes(getRecord(schema3, "name1"));
    byte[] record4 = recordToBytes(getRecord(schema4, "name1"));

    // no injection when the latest schema is not updated
    Assert.assertEquals(schemaChangeInjector.getSchemaIdCache().size(), 0);
    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<byte[]>(record1), wus));
    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<byte[]>(record1), wus));
    Assert.assertEquals(schemaChangeInjector.getSchemaIdCache().size(), 1);


    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<byte[]>(record2), wus));
    Assert.assertEquals(schemaChangeInjector.getSchemaIdCache().size(), 2);

    // updating the latest schema should result in an injection
    schemaChangeInjector.getSchemaRegistry().register("topic1", schema4);

    Iterable<ControlMessage<byte[]>> iterable =
        schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<byte[]>(record3), wus);

    Assert.assertNotNull(iterable);

    List<ControlMessage<byte[]>> controlMessages = Lists.newArrayList(iterable);

    Assert.assertEquals(controlMessages.size(), 2);

    // Should not see any injects since no schema update after the last call
    Assert.assertNull(schemaChangeInjector.injectControlMessagesBefore(new RecordEnvelope<byte[]>(record4), wus));

  }


  private byte[] generateIdBytes(Schema schema) {
    try {
      byte[] schemaBytes = schema.toString().getBytes("UTF-8");
      return MessageDigest.getInstance("MD5").digest(schemaBytes);
    } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Unexpected error trying to convert schema to bytes", e);
    }
  }

  private GenericRecord getRecord(Schema schema, String name) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", name);

    return record;
  }

  private byte[] recordToBytes(GenericRecord record) throws IOException {
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
    EncoderFactory encoderFactory = new EncoderFactory();
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    os.write(0);
    os.write(generateIdBytes(record.getSchema()));
    BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(os, null);
    datumWriter.setSchema(record.getSchema());
    datumWriter.write(record, binaryEncoder);
    binaryEncoder.flush();

    return os.toByteArray();
  }
}
