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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ForkOperatorUtils;

import com.google.common.base.Optional;


/**
 * A {@link TimeBasedWriterPartitioner} for {@link GenericRecord}s.
 *
 * The {@link org.apache.avro.Schema.Field} that contains the timestamp can be specified using
 * {@link TimeBasedAvroWriterPartitioner#WRITER_PARTITION_COLUMNS}, and multiple values can be specified, e.g., "header.timestamp,device.timestamp".
 * The {@link TimeBasedAvroWriterPartitioner#WRITER_PARTITION_COLUMNS_PATTERN} that contains format of column when you have a date_time as string.
 * format use {@link DateTimeFormatter} on default UTC, e.g., "yyyy-MM-dd HH:mm:ss:SSS"
 * The {@link TimeBasedAvroWriterPartitioner#WRITER_PARTITION_COLUMNS_TIMEZONE} that contains timezone of column to convert date_time.
 *
 * If multiple values are specified, they will be tried in order. In the above example, if a record contains a valid
 * "header.timestamp" field, its value will be used, otherwise "device.timestamp" will be used.
 *
 *
 * If a record contains none of the specified fields, or if no field is specified, the current timestamp will be used.
 */
public class TimeBasedAvroWriterPartitioner extends TimeBasedWriterPartitioner<GenericRecord> {

  public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";
  public static final String WRITER_PARTITION_COLUMNS_PATTERN =
      ConfigurationKeys.WRITER_PREFIX + ".partition.columns.pattern";
  public static final String WRITER_PARTITION_COLUMNS_TIMEZONE =
      ConfigurationKeys.WRITER_PREFIX + ".partition.columns.timezone";
  private static final String UTC = "UTC";

  private final Optional<List<String>> partitionColumns;
  private final Optional<String> partitionColumnPattern;
  private final Optional<String> partitionColumnTimeZone;
  private DateTimeFormatter partitionColumnDateTimeFormatter;

  public TimeBasedAvroWriterPartitioner(State state) {
    this(state, 1, 0);
  }

  public TimeBasedAvroWriterPartitioner(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);
    this.partitionColumnPattern = getPropertyAsString(WRITER_PARTITION_COLUMNS_PATTERN, state, numBranches, branchId);
    this.partitionColumnTimeZone = getPropertyAsString(WRITER_PARTITION_COLUMNS_TIMEZONE, state, numBranches, branchId);
    if (this.partitionColumnPattern.isPresent()) {
      this.partitionColumnDateTimeFormatter = DateTimeFormatter.ofPattern(partitionColumnPattern.get());
    }
  }

  private static Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
    return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>>absent();
  }

  private static Optional<String> getPropertyAsString(String property, State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(property, numBranches, branchId);
    return state.contains(propName) ? Optional.of(state.getProp(propName)) : Optional.absent();
  }

  @Override
  public long getRecordTimestamp(GenericRecord record) {
    return getRecordTimestamp(getWriterPartitionColumnValue(record));
  }

  /**
   *  Check if the partition column value is present and is a Long object. Otherwise, use current system time.
   */
  private long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {
    long ret = System.currentTimeMillis();
    if (!this.partitionColumnPattern.isPresent()) {
      if (writerPartitionColumnValue.orNull() instanceof Long) {
        ret = (Long) writerPartitionColumnValue.get();
      }
    } else {
      if (writerPartitionColumnValue.orNull() instanceof String) {
        ret = LocalDateTime.parse((String) writerPartitionColumnValue.get(), partitionColumnDateTimeFormatter)
            .atZone(ZoneId.of(partitionColumnTimeZone.or(UTC))).toInstant().toEpochMilli();
      }
    }
    return ret;
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
