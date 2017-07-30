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

package gobblin.writer.partitioner;

import java.util.List;

import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;
import gobblin.util.ForkOperatorUtils;


/**
 * A {@link TimeBasedWriterPartitioner} for {@link GenericRecord}s.
 *
 * The {@link org.apache.avro.Schema.Field} that contains the timestamp can be specified using
 * {@link WRITER_PARTITION_COLUMNS}, and multiple values can be specified, e.g., "header.timestamp,device.timestamp".
 *
 * If multiple values are specified, they will be tried in order. In the above example, if a record contains a valid
 * "header.timestamp" field, its value will be used, otherwise "device.timestamp" will be used.
 *
 * If a record contains none of the specified fields, or if no field is specified, the current timestamp will be used.
 */
public class TimeBasedAvroWriterPartitioner extends TimeBasedWriterPartitioner<GenericRecord> {

  public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";

  private final Optional<List<String>> partitionColumns;

  public TimeBasedAvroWriterPartitioner(State state) {
    this(state, 1, 0);
  }

  public TimeBasedAvroWriterPartitioner(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);
  }

  private static Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
    return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>> absent();
  }

  @Override
  public long getRecordTimestamp(GenericRecord record) {
    return getRecordTimestamp(getWriterPartitionColumnValue(record));
  }

  /**
   *  Check if the partition column value is present and is a Long object. Otherwise, use current system time.
   */
  private static long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {
    return writerPartitionColumnValue.orNull() instanceof Long ? (Long) writerPartitionColumnValue.get()
        : System.currentTimeMillis();
  }

  /**
   * Retrieve the value of the partition column field specified by this.partitionColumns
   */
  private Optional<Object> getWriterPartitionColumnValue(GenericRecord record) {
    if (!this.partitionColumns.isPresent()) {
      return Optional.absent();
    }

    for (String partitionColumn : this.partitionColumns.get()) {
      Optional<Object> fieldValue = AvroUtils.getFieldValue(record, partitionColumn);
      if (fieldValue.isPresent()) {
        return fieldValue;
      }
    }
    return Optional.absent();
  }
}
