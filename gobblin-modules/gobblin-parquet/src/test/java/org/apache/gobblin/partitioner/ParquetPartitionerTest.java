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
package org.apache.gobblin.partitioner;

import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.converter.parquet.ParquetGroup;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import parquet.schema.GroupType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

import gobblin.configuration.State;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.gobblin.partitioner.ParquetPartitioner.DEFAULT_WRITER_PARQUET_PARTITION_KEY_DEFAULT;
import static org.apache.gobblin.partitioner.ParquetPartitioner.WRITER_PARQUET_PARTITION_KEY;
import static org.apache.gobblin.partitioner.ParquetPartitioner.WRITER_PARQUET_PARTITION_KEY_DEFAULT;
import static org.apache.gobblin.util.ForkOperatorUtils.getPropertyNameForBranch;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static parquet.schema.Type.Repetition.REQUIRED;


@Test(groups = {"gobblin.writer.partitioner"})
public class ParquetPartitionerTest {
  private static final int NUM_OF_BRANCHES = 1;
  private static final int BRANCH_ID = 1;
  private ParquetPartitioner partitioner;
  private GroupType schemaForRecords;

  @BeforeMethod
  public void setUp() {
    PrimitiveType typeForProductName = createType(PrimitiveTypeName.INT64, "name");
    PrimitiveType typeForProductId = createType(PrimitiveTypeName.INT64, "product_id");
    PrimitiveType typeForRegionId = createType(PrimitiveTypeName.INT64, "region_id");
    PrimitiveType typeForCost = createType(PrimitiveTypeName.DOUBLE, "cost");
    PrimitiveType typeForFileId = createType(PrimitiveTypeName.BINARY, "file_id");
    List<Type> types = asList(typeForProductName, typeForProductId, typeForRegionId, typeForCost, typeForFileId);
    schemaForRecords = new GroupType(REQUIRED, "products", types);
  }

  private ParquetPartitioner createPartitioner(String partKeys) {
    State state = mock(State.class);
    String partKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY, NUM_OF_BRANCHES, BRANCH_ID);
    String defPartKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY_DEFAULT, NUM_OF_BRANCHES, BRANCH_ID);
    when(state.getProp(partKeyName)).thenReturn(partKeys);
    when(state.getProp(defPartKeyName, DEFAULT_WRITER_PARQUET_PARTITION_KEY_DEFAULT)).thenReturn("NA");
    return new ParquetPartitioner(state, NUM_OF_BRANCHES, BRANCH_ID);
  }

  @Test
  public void testParquetGroupsPartitionedByMultipleKeys() {
    partitioner = createPartitioner("region_id, file_id");
    ParquetGroup record1 = buildRecord("MacBook", 1, 5, 500.90, "20170505T013500");
    ParquetGroup record2 = buildRecord("MacBook Air", 5, 5, 5090.90, "20170505T013500");
    ParquetGroup record3 = buildRecord("MacBook Air2", 6, 1, 5090.90, "20170505T013500");
    ParquetGroup record4 = buildRecord("MacBook Air3", 7, 1, 5090.90, "20170405T013500");
    ParquetGroup record5 = buildRecord("MacBook Air4", 8, 10, 5090.90, "20170405T013500");
    ParquetGroup record6 = buildRecordWithoutRegion("MacBook Air4", 8, 5090.90);
    GenericRecord part1 = buildPartition("region_id", 5, "file_id", "20170505T013500");
    GenericRecord part2 = buildPartition("region_id", 1, "file_id", "20170505T013500");
    GenericRecord part3 = buildPartition("region_id", 1, "file_id", "20170504T013500");
    GenericRecord part4 = buildPartition("region_id", 10, "file_id", "20170405T013500");
    GenericRecord partDefault = buildPartition("region_id", "NA", "file_id", "NA");
    List<GenericRecord> expectedPartitionsForEachRecord = asList(part1, part1, part2, part3, part4, partDefault);
    List<ParquetGroup> inputRecords = asList(record1, record2, record3, record4, record5, record6);

    List<GenericRecord> partitions =
        inputRecords.stream().map(e -> partitioner.partitionForRecord(e)).collect(toList());
    Assert.assertEquals(partitions, expectedPartitionsForEachRecord);
  }

  @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Partition key 'non_existing_key' not found in the parquet schema required group products.*")
  public void testExceptionOnPartitionKeyNotFoundInParquetSchema() {
    partitioner = createPartitioner("non_existing_key");
    ParquetGroup record1 = buildRecord("MacBook", 1, 5, 500.90, "20170505T013500");
    partitioner.partitionForRecord(record1);
  }

  @Test
  public void testParquetGroupsPartitionedByAKey() {
    partitioner = createPartitioner("region_id");
    ParquetGroup record1 = buildRecord("MacBook", 1, 5, 500.90, "20170505T013500");
    ParquetGroup record2 = buildRecord("MacBook Air", 5, 5, 5090.90, "20170505T013500");
    ParquetGroup record3 = buildRecord("MacBook Air2", 6, 1, 5090.90, "20170505T013500");
    ParquetGroup record4 = buildRecord("MacBook Air3", 7, 1, 5090.90, "20170505T013500");
    ParquetGroup record5 = buildRecord("MacBook Air4", 8, 10, 5090.90, "20170505T013500");
    ParquetGroup record6 = buildRecordWithoutRegion("MacBook Air4", 8, 5090.90);
    GenericRecord partFive = buildPartition("region_id", 5);
    GenericRecord partOne = buildPartition("region_id", 1);
    GenericRecord partTen = buildPartition("region_id", 10);
    GenericRecord partDefault = buildPartition("region_id", "NA");
    List<GenericRecord> expectedPartitionsForEachRecord = asList(partFive, partFive, partOne, partOne, partTen, partDefault);
    List<ParquetGroup> inputRecords = asList(record1, record2, record3, record4, record5, record6);

    List<GenericRecord> partitions =
        inputRecords.stream().map(e -> partitioner.partitionForRecord(e)).collect(toList());
    Assert.assertEquals(partitions, expectedPartitionsForEachRecord);
  }

  private GenericRecord buildPartition(String partitionKey, Object value) {
    GenericRecord partition = new GenericData.Record(partitioner.partitionSchema());
    partition.put(partitionKey, value);
    return partition;
  }

  private GenericRecord buildPartition(String partitionKey1, Object value1, String partitionKey2, Object value2) {
    GenericRecord partition = new GenericData.Record(partitioner.partitionSchema());
    partition.put(partitionKey1, value1);
    partition.put(partitionKey2, value2);
    return partition;
  }

  private ParquetGroup buildRecord(String productName, int productId, int regionId, double cost, String file_id) {
    ParquetGroup record = new ParquetGroup(schemaForRecords);
    record.add("name", productName);
    record.add("product_id", productId);
    record.add("region_id", regionId);
    record.add("cost", cost);
    record.add("file_id", file_id);
    return record;
  }

  private ParquetGroup buildRecordWithoutRegion(String productName, int productId, double cost) {
    ParquetGroup record = new ParquetGroup(schemaForRecords);
    record.add("name", productName);
    record.add("product_id", productId);
    record.add("cost", cost);
    return record;
  }

  private PrimitiveType createType(PrimitiveTypeName typeName, String name) {
    return new PrimitiveType(REQUIRED, typeName, name);
  }
}