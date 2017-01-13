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

package gobblin.couchbase.writer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.couchbase.CouchbaseTestServer;
import gobblin.couchbase.common.TupleDocument;
import gobblin.couchbase.converter.AvroToCouchbaseTupleConverter;
import gobblin.test.TestUtils;


public class CouchbaseWriterTest {

  private CouchbaseTestServer _couchbaseTestServer;
  private CouchbaseEnvironment _couchbaseEnvironment;

  @BeforeSuite
  public void startServers()
  {
    _couchbaseTestServer = new CouchbaseTestServer(TestUtils.findFreePort());
    _couchbaseTestServer.start();

    _couchbaseEnvironment = DefaultCouchbaseEnvironment.builder().bootstrapHttpEnabled(true)
        .bootstrapHttpDirectPort(_couchbaseTestServer.getPort())
        .bootstrapCarrierDirectPort(_couchbaseTestServer.getServerPort())
        .bootstrapCarrierEnabled(false)
        .kvTimeout(10000)
        .build();
  }

  @AfterSuite
  public void stopServers()
  {
    _couchbaseTestServer.stop();
  }

  @Test
  public void testTupleDocumentWrite()
      throws IOException, DataConversionException {
    Properties props = new Properties();
    props.setProperty(CouchbaseWriterConfigurationKeys.BUCKET, "default");
    Config config = ConfigFactory.parseProperties(props);

    CouchbaseWriter writer = new CouchbaseWriter(_couchbaseEnvironment, config);

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
    dataRecord.put("data", testContent.getBytes(Charset.forName("UTF-8")));
    dataRecord.put("flags", 0);

    testRecord.put("key", "hello");
    testRecord.put("data", dataRecord);

    Converter<Schema, String, GenericRecord, TupleDocument> recordConverter = new AvroToCouchbaseTupleConverter();

    TupleDocument doc = recordConverter.convertRecord("", testRecord, null).iterator().next();
    writer.write(doc);

    TupleDocument returnDoc = writer.getBucket().get("hello", TupleDocument.class);

    byte[] returnedBytes = new byte[returnDoc.content().value1().readableBytes()];
    returnDoc.content().value1().readBytes(returnedBytes);
    Assert.assertEquals(returnedBytes, testContent.getBytes(Charset.forName("UTF-8")));

    int returnedFlags = returnDoc.content().value2();
    Assert.assertEquals(returnedFlags, 0);

  }


}
