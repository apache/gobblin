/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer.partitioner;

import java.util.List;

import com.google.common.base.Optional;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.writer.partitioner.TimeBasedWriterPartitioner;


/**
 * A {@link TimeBasedWriterPartitioner} for {@link JsonObject}s.
 *
 * The {@link com.google.gson.JsonElement} that contains the timestamp can be specified using
 * {@link WRITER_PARTITION_COLUMNS}, and multiple values can be specified, e.g., "header.timestamp,device.timestamp".
 *
 * If multiple values are specified, they will be tried in order. In the above example, if a record contains a valid
 * "header.timestamp" field, its value will be used, otherwise "device.timestamp" will be used.
 *
 * If a record contains none of the specified fields, or if no field is specified, the current timestamp will be used.
 */
public class TimeBasedJsonWriterPartitioner extends TimeBasedWriterPartitioner<JsonElement> {

  public static final String WRITER_PARTITION_COLUMNS = ConfigurationKeys.WRITER_PREFIX + ".partition.columns";

  public static final String WRITER_TIMESTAMP_IN_UNIX = ConfigurationKeys.WRITER_PREFIX + ".partition.timestamp.unix";


  private final Optional<List<String>> partitionColumns;
  private final boolean isUnixTimestamp;
  private final static int UNIX_TIMESTAMP_MULTIPLIER = 1000;

  public TimeBasedJsonWriterPartitioner(State state) {
    this(state, 1, 0);
  }

  public TimeBasedJsonWriterPartitioner(State state, int numBranches, int branchId) {
    super(state, numBranches, branchId);
    this.partitionColumns = getWriterPartitionColumns(state, numBranches, branchId);
    this.isUnixTimestamp = isTimestampUnixFormat(state, numBranches, branchId);
  }

  private static Optional<List<String>> getWriterPartitionColumns(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_COLUMNS, numBranches, branchId);
    return state.contains(propName) ? Optional.of(state.getPropAsList(propName)) : Optional.<List<String>> absent();
  }

  private static Boolean isTimestampUnixFormat(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_TIMESTAMP_IN_UNIX, numBranches, branchId);
    return state.getPropAsBoolean(propName, Boolean.FALSE);
  }

  @Override
  public long getRecordTimestamp(JsonElement record) {
      return getRecordTimestamp(getWriterPartitionColumnValue(record));
  }

  /**
   *  Check if the partition column value is present and is a Long object. Otherwise, use current system time.
   */
  private long getRecordTimestamp(Optional<JsonElement> writerPartitionColumnValue) {
    int multiplier = 1;

    //Unix timestamp is second precision and java timestamp is millisecond
    if (this.isUnixTimestamp) {
        multiplier = UNIX_TIMESTAMP_MULTIPLIER;
    }

    return writerPartitionColumnValue.isPresent() ? (Long) writerPartitionColumnValue.get().getAsLong()*multiplier
        : System.currentTimeMillis();
  }

  /**
   * Retrieve the value of the partition column field specified by this.partitionColumns
   */
  private Optional<JsonElement> getWriterPartitionColumnValue(JsonElement record) {
    if (!this.partitionColumns.isPresent()) {
      return Optional.absent();
    }

    for (String partitionColumn : this.partitionColumns.get()) {
      if (record.isJsonObject()){
        JsonElement fieldValue = record.getAsJsonObject().get(partitionColumn);
        if (fieldValue != null) {
          return Optional.of(fieldValue);
        }

      }
    }
    return Optional.absent();
  }
}
