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
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.parquet.ParquetGroup;
import org.apache.gobblin.writer.partitioner.WriterPartitioner;

import parquet.example.data.Group;
import parquet.schema.GroupType;

import static org.apache.avro.Schema.Type.STRING;
import static org.apache.gobblin.configuration.ConfigurationKeys.WRITER_PREFIX;
import static org.apache.gobblin.util.ForkOperatorUtils.getPropertyNameForBranch;


/**
 * A Partitioner for parquet {@link Group} records by a provided partition key.
 * Partition key can be set by using {@value #WRITER_PARQUET_PARTITION_KEY} in the job configuration.
 * Multiple partition keys can be set by comma separated. Nested partition keys should be dot separated.
 * Ex writer.partitioner.parquet.key="type,payload.pusher_type"
 * If the partition key not found in {@link Group} for some records.
 * Then the default partition value can be set by {@value #WRITER_PARQUET_PARTITION_KEY_DEFAULT}
 * in the job configuration.
 * @author tilakpatidar
 */
public class ParquetPartitioner implements WriterPartitioner<ParquetGroup> {
  public static final String WRITER_PARQUET_PARTITION_KEY = WRITER_PREFIX + ".partitioner.parquet.key";
  public static final String WRITER_PARQUET_PARTITION_KEY_DEFAULT =
      WRITER_PREFIX + ".partitioner.parquet.key.defaultKey";
  public static final String DEFAULT_WRITER_PARQUET_PARTITION_KEY_DEFAULT = "NON_PARTITIONED";
  private final List<PartitionKey> partitionKey;
  private final String partitionKeyForNonMatchedRecords;

  public ParquetPartitioner(State state, int numBranches, int branchId) {
    String partKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY, numBranches, branchId);
    String defPartKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY_DEFAULT, numBranches, branchId);
    String delimitedPartKey = state.getProp(partKeyName);
    this.partitionKey = Optional.ofNullable(delimitedPartKey).map(PartitionKey::partitionKeys).orElseGet(() -> {
      throw new RuntimeException(String.format("Partition key not found in property %s", WRITER_PARQUET_PARTITION_KEY));
    });
    this.partitionKeyForNonMatchedRecords = state.getProp(defPartKeyName, DEFAULT_WRITER_PARQUET_PARTITION_KEY_DEFAULT);
  }

  @Override
  public Schema partitionSchema() {
    FieldAssembler<Schema> fieldAssembler =
        SchemaBuilder.record("Schema").namespace("gobblin.writer.partitioner").fields();
    for (PartitionKey aPartitionKey : partitionKey) {
      fieldAssembler = fieldAssembler.name(aPartitionKey.getFlattenedKey()).type(Schema.create(STRING)).noDefault();
    }
    Schema schema = fieldAssembler.endRecord();
    return schema;
  }

  @Override
  public GenericRecord partitionForRecord(ParquetGroup record) {
    GenericRecord partition = new GenericData.Record(partitionSchema());
    GroupType groupType = record.getSchema();
    for (PartitionKey partKey : partitionKey) {
      try {
        String[] partSubKeys = partKey.getSubKeys();
        partition.put(partKey.getFlattenedKey(), traverseNestedPartitionKey(record, groupType, partSubKeys));
      } catch (RuntimeException e) {
        partition.put(partKey.getFlattenedKey(), partitionKeyForNonMatchedRecords);
      }
    }
    return partition;
  }

  private Object traverseNestedPartitionKey(ParquetGroup record, GroupType groupType, String[] partSubKeys) {
    GroupType gt = groupType;
    Group rc = record;
    for (String partSubKey : partSubKeys) {
      Integer index = gt.getFieldIndex(partSubKey);
      if (gt.getType(index).isPrimitive()) {
        return rc.getValueToString(index, 0);
      } else {
        rc = rc.getGroup(index, 0);
        gt = gt.getFields().get(index).asGroupType();
      }
    }
    return partitionKeyForNonMatchedRecords;
  }
}
