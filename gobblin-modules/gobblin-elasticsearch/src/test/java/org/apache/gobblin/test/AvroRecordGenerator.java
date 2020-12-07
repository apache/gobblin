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
package org.apache.gobblin.test;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.elasticsearch.typemapping.AvroGenericRecordTypeMapper;


/**
 * A generator of Avro records of type {@link GenericRecord}
 */
public class AvroRecordGenerator implements RecordTypeGenerator<GenericRecord> {
  @Override
  public String getName() {
    return "avro";
  }

  @Override
  public String getTypeMapperClassName() {
    return AvroGenericRecordTypeMapper.class.getCanonicalName();
  }

  @Override
  public GenericRecord getRecord(String id, PayloadType payloadType) {
    GenericRecord record = getTestAvroRecord(id, payloadType);
    return record;
  }

  static GenericRecord getTestAvroRecord(String identifier, PayloadType payloadType) {
    Schema dataRecordSchema =
        SchemaBuilder.record("Data").fields().name("data").type().bytesType().noDefault().name("flags").type().intType()
            .noDefault().endRecord();

    Schema schema;
    Object payloadValue;
    switch (payloadType) {
      case STRING: {
        schema = SchemaBuilder.record("TestRecord").fields()
            .name("id").type().stringType().noDefault()
            .name("key").type().stringType().noDefault()
            .name("data").type(dataRecordSchema).noDefault()
            .endRecord();
        payloadValue = TestUtils.generateRandomAlphaString(20);
        break;
      }
      case LONG: {
        schema = SchemaBuilder.record("TestRecord").fields()
            .name("id").type().stringType().noDefault()
            .name("key").type().longType().noDefault()
            .name("data").type(dataRecordSchema).noDefault()
            .endRecord();
        payloadValue = TestUtils.generateRandomLong();
        break;
      }
      case MAP: {
        schema = SchemaBuilder.record("TestRecord").fields()
            .name("id").type().stringType().noDefault()
            .name("key").type().map().values().stringType().noDefault()
            .name("data").type(dataRecordSchema).noDefault()
            .endRecord();
        payloadValue = Collections.EMPTY_MAP;
        break;
      }
      default: {
        throw new RuntimeException("Do not know how to handle this time");
      }
    }

    GenericData.Record testRecord = new GenericData.Record(schema);

    String testContent = "hello world";

    GenericData.Record dataRecord = new GenericData.Record(dataRecordSchema);
    dataRecord.put("data", ByteBuffer.wrap(testContent.getBytes(Charset.forName("UTF-8"))));
    dataRecord.put("flags", 0);

    testRecord.put("key", payloadValue);
    testRecord.put("data", dataRecord);
    testRecord.put("id", identifier);
    return testRecord;
  }

}
