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
package org.apache.gobblin.parquet.writer.test;

import java.util.Arrays;

import org.apache.avro.generic.GenericRecord;

import com.google.protobuf.Message;

import org.apache.gobblin.parquet.writer.ParquetRecordFormat;
import org.apache.gobblin.test.TestRecord;
import org.apache.gobblin.test.proto.TestRecordProtos;


/**
 * Holder for TestConstantsBase
 * @param <ParquetGroup> : the class that implements ParquetGroup, generic to allow package-specific overrides
 */
public abstract class TestConstantsBase<ParquetGroup> {

  public static TestRecord[] getTestValues() {
    return Arrays.copyOf(TEST_VALUES, TEST_VALUES.length);
  }

  public static String[] getPayloadValues() {
    return Arrays.copyOf(PAYLOAD_VALUES, PAYLOAD_VALUES.length);
  }

  public static int[] getSequenceValues() {
    return Arrays.copyOf(SEQUENCE_VALUES, SEQUENCE_VALUES.length);
  }

  public static int[] getPartitionValues() {
    return Arrays.copyOf(PARTITION_VALUES, PARTITION_VALUES.length);
  }

  public final String getParquetTestFilename(String format) { return "test-"+format+".parquet"; };

  public static final String TEST_FS_URI = "file:///";

  public static final String TEST_ROOT_DIR = System.getProperty("java.io.tmpdir") + "/" + System.currentTimeMillis();

  public static final String TEST_STAGING_DIR = TEST_ROOT_DIR + "/staging";

  public static final String TEST_OUTPUT_DIR = TEST_ROOT_DIR + "/output";

  public static final String TEST_WRITER_ID = "writer-1";

  public static final String TEST_EXTRACT_NAMESPACE = "com.linkedin.writer.test";

  public static final String TEST_EXTRACT_ID = String.valueOf(System.currentTimeMillis());

  public static final String TEST_EXTRACT_TABLE = "TestTable";

  public static final String TEST_EXTRACT_PULL_TYPE = "FULL";


  private static final TestRecord[] TEST_VALUES = new TestRecord[2];
  public static final String PAYLOAD_FIELD_NAME = "payload";
  public static final String SEQUENCE_FIELD_NAME = "sequence";
  public static final String PARTITION_FIELD_NAME = "partition";
  private static final String[] PAYLOAD_VALUES = {"value1", "value2"};
  private static final int[] SEQUENCE_VALUES = {1, 2};
  private static final int[] PARTITION_VALUES = {0, 1};
  static {
    for (int i=0; i < 2; ++i) {
      TestRecord record = new TestRecord(getPartitionValues()[i],
          getSequenceValues()[i],
          getPayloadValues()[i]);
      TEST_VALUES[i] = record;
    }
  }


  public Object getRecord(int index, ParquetRecordFormat format) {
    switch (format) {
      case GROUP: {
        return convertToParquetGroup(getTestValues()[index]);
      }
      case PROTOBUF: {
        return getProtobufMessage(getTestValues()[index]);
      }
      case AVRO: {
        return getAvroMessage(getTestValues()[index]);
      }
      default: {
        throw new RuntimeException("Not understanding format " + format);
      }
    }
  }


  public abstract ParquetGroup convertToParquetGroup(TestRecord record);


  private Message getProtobufMessage(TestRecord testValue) {
    return TestRecordProtos.TestRecord.newBuilder()
        .setPayload(testValue.getPayload())
        .setPartition(testValue.getPartition())
        .setSequence(testValue.getSequence())
        .build();
  }

  private GenericRecord getAvroMessage(TestRecord record) {
    org.apache.gobblin.test.avro.TestRecord testRecord = new org.apache.gobblin.test.avro.TestRecord();
    testRecord.setPayload(record.getPayload());
    testRecord.setPartition(record.getPartition());
    testRecord.setSequence(record.getSequence());
    return testRecord;
  }



}
