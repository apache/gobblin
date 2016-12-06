/*
 *
 *  * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 *  * this file except in compliance with the License. You may obtain a copy of the
 *  * License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed
 *  * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 *  * CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package gobblin.couchbase.writer;

import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.couchbase.common.TupleDocument;
import gobblin.couchbase.converter.AvroToCouchbaseTupleConverter;


public class CouchbaseWriterTest {


  @Test
  public void testStringWrite()
      throws IOException, DataConversionException {
    Properties props = new Properties();
    props.setProperty(CouchbaseWriterConfigurationKeys.BOOTSTRAP_SERVERS, "localhost");
    props.setProperty(CouchbaseWriterConfigurationKeys.BUCKET, "default");
    Config config = ConfigFactory.parseProperties(props);

    CouchbaseWriter writer = new CouchbaseWriter(config);

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


    GenericData.Record dataRecord = new GenericData.Record(dataRecordSchema);
    dataRecord.put("data", "hello world".getBytes());
    dataRecord.put("flags", 0);

    testRecord.put("key", "hello");
    testRecord.put("data", dataRecord);

    Converter<Schema, String, GenericRecord, TupleDocument> recordConverter = new AvroToCouchbaseTupleConverter();

    TupleDocument doc = recordConverter.convertRecord("", testRecord, null).iterator().next();
    writer.write(doc);

  }

}
