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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.converter.parquet.ParquetGroup;
import org.apache.gobblin.writer.partitioner.WriterPartitioner;

import parquet.example.data.Group;
import parquet.io.InvalidRecordException;
import parquet.schema.GroupType;

import static org.apache.avro.Schema.Field.Order.IGNORE;
import static org.apache.avro.Schema.Type.STRING;
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
  private final String[] partitionKey;
  private final String partitionKeyForNonMatchedRecords;

  public ParquetPartitioner(State state, int numBranches, int branchId) {
    String partKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY, numBranches, branchId);
    String defPartKeyName = getPropertyNameForBranch(WRITER_PARQUET_PARTITION_KEY_DEFAULT, numBranches, branchId);
    String delimetedPartKey = state.getProp(partKeyName);
    this.partitionKey =
        Optional.ofNullable(delimetedPartKey).map(e -> e.replaceAll("\\s*,\\s*", ",").split(",")).orElseGet(() -> {
          throw new RuntimeException(
              String.format("Partition key not found in property %s", WRITER_PARQUET_PARTITION_KEY));
        });
    this.partitionKeyForNonMatchedRecords = state.getProp(defPartKeyName, DEFAULT_WRITER_PARQUET_PARTITION_KEY_DEFAULT);
  }

  @Override
  public Schema partitionSchema() {
    List<Field> primitiveTypes = new ArrayList<>(partitionKey.length);
    for (String e : partitionKey) {
      Field field = new Field(e.trim(), Schema.create(STRING), "", null, IGNORE);
      primitiveTypes.add(field);
    }
    return Schema.createRecord("ParquetPartitionerSchema", "", "gobblin.partitioner", false, primitiveTypes);
  }

  @Override
  public GenericRecord partitionForRecord(ParquetGroup record) {
    GenericRecord partition = new GenericData.Record(partitionSchema());
    GroupType groupType = record.getSchema();
    for (String partKey : partitionKey) {
      try {
        Integer index = groupType.getFieldIndex(partKey);
        try {
          String partitionValue = record.getValueToString(index, 0);
          partition.put(partKey, partitionValue);
        } catch (RuntimeException e) {
          partition.put(partKey, partitionKeyForNonMatchedRecords);
        }
      } catch (InvalidRecordException e) {
        String schema = record.getSchema().toString();
        String errMsg = String.format("Partition key '%s' not found in the parquet schema %s", partKey, schema);
        throw new RuntimeException(errMsg);
      }
    }
    return partition;
  }
}
