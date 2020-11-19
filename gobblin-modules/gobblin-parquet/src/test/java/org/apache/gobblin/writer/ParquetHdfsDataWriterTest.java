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
package org.apache.gobblin.writer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.proto.ProtoParquetReader;
import org.apache.parquet.schema.MessageType;

import org.apache.gobblin.parquet.writer.ParquetRecordFormat;
import org.apache.gobblin.parquet.writer.test.ParquetHdfsDataWriterTestBase;
import org.apache.gobblin.test.TestRecord;
import org.apache.gobblin.test.proto.TestRecordProtos;


@Test(groups = {"gobblin.writer"})
public class ParquetHdfsDataWriterTest extends ParquetHdfsDataWriterTestBase {

  public ParquetHdfsDataWriterTest() {
    super(new TestConstants());
  }

  @BeforeMethod
  public void setUp()
      throws Exception {
    super.setUp();
  }

  protected DataWriterBuilder getDataWriterBuilder() {
    return new ParquetDataWriterBuilder();
  }

  @Override
  protected List<TestRecord> readParquetRecordsFromFile(File outputFile, ParquetRecordFormat format)
      throws IOException {
    switch (format) {
      case GROUP: {
        return readParquetFilesGroup(outputFile);
      }
      case PROTOBUF: {
        return readParquetFilesProto(outputFile);
      }
      case AVRO: {
        return readParquetFilesAvro(outputFile);
      }
      default: throw new RuntimeException(format + " is not supported");
    }
  }

  private List<TestRecord> readParquetFilesAvro(File outputFile)
      throws IOException {
    ParquetReader<org.apache.gobblin.test.avro.TestRecord> reader = null;
    List<TestRecord> records = new ArrayList<>();
    try {
      reader = new AvroParquetReader<>(new Path(outputFile.toString()));
      for (org.apache.gobblin.test.avro.TestRecord value = reader.read(); value != null; value = reader.read()) {
        records.add(new TestRecord(value.getPartition(),
            value.getSequence(),
            value.getPayload()));
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ex) {
          System.out.println(ex.getMessage());
        }
      }
    }
    return records;

  }


  protected List<TestRecord> readParquetFilesProto(File outputFile)
      throws IOException {
    ParquetReader<TestRecordProtos.TestRecordOrBuilder> reader = null;
    List<TestRecord> records = new ArrayList<>();
    try {
      reader = new ProtoParquetReader<>(new Path(outputFile.toString()));
      TestRecordProtos.TestRecordOrBuilder value = reader.read();
      while (value!= null) {
        records.add(new TestRecord(value.getPartition(),
            value.getSequence(),
            value.getPayload()));
        value = reader.read();
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ex) {
          System.out.println(ex.getMessage());
        }
      }
    }
    return records;
  }

  protected List<TestRecord> readParquetFilesGroup(File outputFile)
      throws IOException {
    ParquetReader<Group> reader = null;
    List<Group> records = new ArrayList<>();
    try {
      reader = new ParquetReader<>(new Path(outputFile.toString()), new SimpleReadSupport());
      for (Group value = reader.read(); value != null; value = reader.read()) {
        records.add(value);
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ex) {
          System.out.println(ex.getMessage());
        }
      }
    }
    return records.stream().map(value -> new TestRecord(
        value.getInteger(TestConstants.PARTITION_FIELD_NAME, 0),
        value.getInteger(TestConstants.SEQUENCE_FIELD_NAME, 0),
        value.getString(TestConstants.PAYLOAD_FIELD_NAME, 0)
    )).collect(Collectors.toList());
  }


  @Test
  public void testWrite()
      throws Exception {
    super.testWrite();
  }

  @Override
  protected Object getSchema(ParquetRecordFormat format) {
    switch (format) {
      case GROUP: {
        return TestConstants.PARQUET_SCHEMA;
      }
      case PROTOBUF: {
        return TestRecordProtos.TestRecord.class;
      }
      case AVRO: {
        return org.apache.gobblin.test.avro.TestRecord.getClassSchema();
      }
      default:
        throw new RuntimeException(format.name() + " is not implemented");
    }
  }


  @AfterClass
  public void tearDown()
      throws IOException {
    super.tearDown();
  }

  class SimpleReadSupport extends ReadSupport<Group> {
    @Override
    public RecordMaterializer<Group> prepareForRead(Configuration conf, Map<String, String> metaData,
        MessageType schema, ReadContext context) {
      return new GroupRecordConverter(schema);
    }

    @Override
    public ReadContext init(InitContext context) {
      return new ReadContext(context.getFileSchema());
    }
  }
}
