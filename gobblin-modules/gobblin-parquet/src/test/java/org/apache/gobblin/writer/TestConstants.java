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

import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroup;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType;
import parquet.schema.Types;


public class TestConstants {
  public static final MessageType PARQUET_SCHEMA = Types.buildMessage()
      .addFields(Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name"),
          Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named("age")).named("User");

  public static final Group PARQUET_RECORD_1 = new SimpleGroup(PARQUET_SCHEMA);

  public static final Group PARQUET_RECORD_2 = new SimpleGroup(PARQUET_SCHEMA);

  public static final String PARQUET_TEST_FILENAME = "test.parquet";

  public static final String TEST_FS_URI = "file:///";

  public static final String TEST_ROOT_DIR = System.getProperty("java.io.tmpdir");

  public static final String TEST_STAGING_DIR = TEST_ROOT_DIR + "/staging";

  public static final String TEST_OUTPUT_DIR = TEST_ROOT_DIR + "/output";

  public static final String TEST_WRITER_ID = "writer-1";

  public static final String TEST_EXTRACT_NAMESPACE = "com.linkedin.writer.test";

  public static final String TEST_EXTRACT_ID = String.valueOf(System.currentTimeMillis());

  public static final String TEST_EXTRACT_TABLE = "TestTable";

  public static final String TEST_EXTRACT_PULL_TYPE = "FULL";

  static {
    PARQUET_RECORD_1.add("name", "tilak");
    PARQUET_RECORD_1.add("age", 22);
    PARQUET_RECORD_2.add("name", "other");
    PARQUET_RECORD_2.add("age", 22);
  }
}
