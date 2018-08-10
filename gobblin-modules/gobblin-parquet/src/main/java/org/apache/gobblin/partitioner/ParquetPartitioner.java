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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.parquet.ParquetGroup;
import org.apache.gobblin.writer.partitioner.WriterPartitioner;

import parquet.example.data.Group;
import parquet.io.InvalidRecordException;

import static org.apache.gobblin.configuration.ConfigurationKeys.WRITER_PREFIX;
import static org.apache.gobblin.util.ForkOperatorUtils.getPropertyNameForBranch;


/**
 * A Partitioner for parquet {@link Group} records by a provided partition key.
 * Partition key can be set by using {@value #WRITER_PARQUET_PARTITION_KEY} in the job configuration.
 * If the partition key not found in {@link Group} for some records.
 * Then the default partition value can be set by {@value #WRITER_PARQUET_PARTITION_KEY_DEFAULT}
 * in the job configuration.
 * @author tilakpatidar
 */
public class ParquetPartitioner implements WriterPartitioner<ParquetGroup> {
  public static final String WRITER_PARQUET_PARTITION_KEY = WRITER_PREFIX + ".partitioner.parquet.key";
  public static final String WRITER_PARQUET_PARTITION_KEY_DEFAULT = WRITER_PREFIX + ".partitioner.parquet.key.default";
  public static final String DEFAULT_WRITER_PARQUET_PARTITION_KEY_DEFAULT = "NON_PARTITIONED";
  private final String partitionKey;
  private final String partitionKeyForNonMatchedRecords;

  public ParquetPartitioner(State state, int numBranches, int branchId) {
    String partKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY, numBranches, branchId);
    String defPartKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY_DEFAULT, numBranches, branchId);
    this.partitionKey = state.getProp(partKeyName);
    this.partitionKeyForNonMatchedRecords = state.getProp(defPartKeyName, DEFAULT_WRITER_PARQUET_PARTITION_KEY_DEFAULT);
  }

  @Override
  public Schema partitionSchema() {
    return SchemaBuilder.record("ParquetPartitionerSchema").namespace("gobblin.partitioner").fields().name(partitionKey)
        .type(Schema.create(Schema.Type.STRING)).noDefault().endRecord();
  }

  @Override
  public GenericRecord partitionForRecord(ParquetGroup record) {
    Integer fieldIndex;
    try {
      fieldIndex = record.getSchema().getFieldIndex(partitionKey);
    } catch (InvalidRecordException e) {
      String schema = record.getSchema().toString();
      String errMsg = String.format("Partition key '%s' not found in the parquet schema %s", partitionKey, schema);
      throw new RuntimeException(errMsg);
    }

    try {
      String partitionValue = record.getValueToString(fieldIndex, 0);
      GenericRecord partition = new GenericData.Record(partitionSchema());
      partition.put(partitionKey, partitionValue);
      return partition;
    } catch (RuntimeException e) {
      return defaultPartition();
    }
  }

  private GenericRecord defaultPartition() {
    GenericRecord partition = new GenericData.Record(partitionSchema());
    partition.put(partitionKey, partitionKeyForNonMatchedRecords);
    return partition;
  }
}
