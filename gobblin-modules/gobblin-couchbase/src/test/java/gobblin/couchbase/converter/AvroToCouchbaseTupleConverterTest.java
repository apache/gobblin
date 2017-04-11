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

package gobblin.couchbase.converter;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.converter.Converter;
import gobblin.couchbase.common.TupleDocument;



public class AvroToCouchbaseTupleConverterTest {

  @Test
  public void testBasicConvert() throws Exception {

    Schema dataRecordSchema = SchemaBuilder.record("Data")
        .fields()
        .name("data").type().bytesType().noDefault()
        .name("flags").type().intType().noDefault()
        .endRecord();

    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("key").type().stringType().noDefault()
        .name("data").type(dataRecordSchema).noDefault()
        .endRecord();

    GenericData.Record testRecord = new GenericData.Record(schema);


    String testContent = "hello world";
    GenericData.Record dataRecord = new GenericData.Record(dataRecordSchema);
    dataRecord.put("data", ByteBuffer.wrap(testContent.getBytes(Charset.forName("UTF-8"))));
    dataRecord.put("flags", 0);

    testRecord.put("key", "hello");
    testRecord.put("data", dataRecord);

    Converter<Schema, String, GenericRecord, TupleDocument> recordConverter = new AvroToCouchbaseTupleConverter();

    TupleDocument returnDoc = recordConverter.convertRecord("", testRecord, null).iterator().next();
    byte[] returnedBytes = new byte[returnDoc.content().value1().readableBytes()];
    returnDoc.content().value1().readBytes(returnedBytes);
    Assert.assertEquals(returnedBytes, testContent.getBytes(Charset.forName("UTF-8")));

    int returnedFlags = returnDoc.content().value2();
    Assert.assertEquals(returnedFlags, 0);

  }

}
