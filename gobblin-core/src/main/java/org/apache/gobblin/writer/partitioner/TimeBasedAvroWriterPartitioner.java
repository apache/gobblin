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

package org.apache.gobblin.writer.partitioner;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ForkOperatorUtils;


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
@Slf4j
public class TimeBasedAvroWriterPartitioner extends TimeBasedWriterPartitioner<GenericRecord> {

  public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";
  public static final String WRITER_PARTITION_ENABLE_PARSE_AS_STRING =
      ConfigurationKeys.WRITER_PREFIX + ".partition.enableParseAsString";

  private final Optional<List<String>> partitionColumns;
  private final boolean enableParseAsString;

  public TimeBasedAvroWriterPartitioner(State state) {
    this(state, 1, 0);
  }

  public TimeBasedAvroWriterPartitioner(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);
    this.enableParseAsString = getEnableParseAsString(state, numBranches, branchId);
    log.info("Enable parse as string: {}", this.enableParseAsString);
  }

  private static Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
    log.info("Partition columns for dataset {} are: {}", state.getProp(ConfigurationKeys.DATASET_URN_KEY),
        state.getProp(propName));
    return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>> absent();
  }

  private static boolean getEnableParseAsString(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_ENABLE_PARSE_AS_STRING,
        numBranches, branchId);
    return state.getPropAsBoolean(propName, false);
  }

  @Override
  public long getRecordTimestamp(GenericRecord record) {
    return getRecordTimestamp(getWriterPartitionColumnValue(record));
  }

  /**
   *  Check if the partition column value is present and is a Long object. Otherwise, use current system time.
   */
  private long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {

    if (writerPartitionColumnValue.isPresent()) {
      Object val = writerPartitionColumnValue.get();
      if (val instanceof Long) {
        return (Long) val;
      } else if (enableParseAsString) {
        return Long.parseLong(val.toString());
      }
    }

    // Default to current time
    return timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
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
