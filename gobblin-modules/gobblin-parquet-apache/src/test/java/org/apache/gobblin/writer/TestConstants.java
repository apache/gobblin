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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import org.apache.gobblin.parquet.writer.test.TestConstantsBase;
import org.apache.gobblin.test.TestRecord;


public class TestConstants extends TestConstantsBase<Group> {

  public static final MessageType PARQUET_SCHEMA = Types.buildMessage()
      .addFields(
          Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8)
              .named(TestConstants.PAYLOAD_FIELD_NAME),
          Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(TestConstants.PARTITION_FIELD_NAME),
          Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(TestConstants.SEQUENCE_FIELD_NAME))
      .named("Data");

  @Override
  public Group convertToParquetGroup(TestRecord record) {
    Group group = new SimpleGroup(PARQUET_SCHEMA);
    group.add(PAYLOAD_FIELD_NAME, record.getPayload());
    group.add(SEQUENCE_FIELD_NAME, Long.valueOf(record.getSequence()));
    group.add(PARTITION_FIELD_NAME, record.getPartition());
    return group;
  }

}
