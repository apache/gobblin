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

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Optional;


/**
 * A {@link TimeBasedWriterPartitioner} for {@link OrcSerde.OrcSerdeRow}s.
 * <p>
 * The {@link OrcSerde.OrcSerdeRow} that contains the timestamp can be specified using
 * {@link TimeBasedORCSerDeWriterPartitioner#WRITER_PARTITION_COLUMN_NAME}.
 * only accept simple value, e.g., "timestamp" or "event_date".
 * The {@link TimeBasedORCSerDeWriterPartitioner#WRITER_PARTITION_COLUMN_INDEX} that contains the index of column, has better performance.
 * The {@link TimeBasedORCSerDeWriterPartitioner#WRITER_PARTITION_COLUMN_PATTERN} that contains format of column when you have a date_time as string.
 * All format use {@link DateTimeFormatter} on UTC, e.g., "yyyy-MM-dd HH:mm:ss:SSS"
 * <p>
 * If a record contains none of the specified fields, or if no field is specified, the current timestamp will be used.
 */
public class TimeBasedORCSerDeWriterPartitioner extends TimeBasedWriterPartitioner<Object> {

    private static final String WRITER_PARTITION_COLUMN_NAME = ConfigurationKeys.WRITER_PREFIX + ".partition.column";
    private static final String WRITER_PARTITION_COLUMN_INDEX = ConfigurationKeys.WRITER_PREFIX + ".partition.column.index";
    private static final String WRITER_PARTITION_COLUMN_PATTERN = ConfigurationKeys.WRITER_PREFIX + ".partition.column.pattern";
    private static final String WRITER_PARTITION_COLUMN_TIMEZONE = ConfigurationKeys.WRITER_PREFIX + ".partition.column.timezone";

    private static final String UTC = "UTC";
    private static final String ORC_SERDE_INSPECTOR = "getInspector";
    private static final String ORC_SERDE_ROW = "realRow";

    private final Optional<String> partitionColumnName;
    private final Optional<Integer> partitionColumnIndex;
    private final Optional<String> partitionColumnPattern;
    private final Optional<String> partitionColumnTimeZone;
    private DateTimeFormatter partitionColumnDateTimeFormatter;

    public TimeBasedORCSerDeWriterPartitioner(State state, int numBranches, int branchId) {
        super(state, numBranches, branchId);
        this.partitionColumnName = getPropertyAsString(WRITER_PARTITION_COLUMN_NAME, state, numBranches, branchId);
        this.partitionColumnIndex = getPropertyAsInteger(WRITER_PARTITION_COLUMN_INDEX, state, numBranches, branchId);
        this.partitionColumnPattern = getPropertyAsString(WRITER_PARTITION_COLUMN_PATTERN, state, numBranches, branchId);
        this.partitionColumnTimeZone = getPropertyAsString(WRITER_PARTITION_COLUMN_TIMEZONE, state, numBranches, branchId);
        if (this.partitionColumnPattern.isPresent()) {
            this.partitionColumnDateTimeFormatter = DateTimeFormatter.ofPattern(partitionColumnPattern.get());
        }
    }

    private static Optional<Integer> getPropertyAsInteger(String property, State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(property, numBranches, branchId);
        return state.contains(propName) ? Optional.of(state.getPropAsInt(propName)) : Optional.empty();
    }

    private static Optional<String> getPropertyAsString(String property, State state, int numBranches, int branchId) {
        String propName = ForkOperatorUtils.getPropertyNameForBranch(property, numBranches, branchId);
        return state.contains(propName) ? Optional.of(state.getProp(propName)) : Optional.empty();
    }

    /**
     * Check if the partition column value is present and is a Long object.
     * if partitionColumnPattern is present we format LocalDateTime as EpochMilli.
     * Otherwise, use current system time.
     */
    private long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {
        long ret = System.currentTimeMillis();
        if (!this.partitionColumnPattern.isPresent()) {
            if (writerPartitionColumnValue.orElse(null) instanceof Long)
                ret = (Long) writerPartitionColumnValue.get();
        } else {
            if (writerPartitionColumnValue.orElse(null) instanceof String)
                ret = LocalDateTime.parse((String) writerPartitionColumnValue.get(), partitionColumnDateTimeFormatter)
                        .atZone(ZoneId.of(partitionColumnTimeZone.orElse(UTC))).toInstant().toEpochMilli();
        }
        return ret;
    }

    @Override
    public long getRecordTimestamp(Object record) {
        return getRecordTimestamp(getWriterPartitionColumnValue(record));
    }

    /**
     * Retrieve the index of the partition column field specified by this.partitionColumnName. Is slowest performance.
     */
    private int extractTimestampIndex(Object orcRecord) {
        if (!this.partitionColumnName.isPresent()) {
            throw new IllegalArgumentException("writer.partition.column doesn't have value");
        }

        Class<?> clazz = orcRecord.getClass();
        try {
            Method method = clazz.getDeclaredMethod(ORC_SERDE_INSPECTOR);
            method.setAccessible(true);
            ObjectInspector inspector = (ObjectInspector) method.invoke(clazz.cast(orcRecord));
            return ((StandardStructObjectInspector) inspector).getStructFieldRef(partitionColumnName.get()).getFieldID();
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new RuntimeException("writer.partition.column: " + partitionColumnName.get() + " doesn't exist on row");
        }
    }

    /**
     * Retrieve the value of the partition column field specified by this.partitionColumnIndex.
     * If partitionColumnIndex not exist, this will seek it.
     */
    private Optional<Object> getWriterPartitionColumnValue(Object orcRecord) {
        if (!this.partitionColumnName.isPresent()) {
            return Optional.empty();
        }

        Class<?> clazz = orcRecord.getClass();
        ArrayList<Object> realRow;

        //only for check if column index is know.
        int timestampIndex = this.partitionColumnIndex.isPresent() ?
                this.partitionColumnIndex.get() : extractTimestampIndex(orcRecord);

        try {
            Field field = clazz.getDeclaredField(ORC_SERDE_ROW);
            field.setAccessible(true);

            realRow = (ArrayList<Object>) field.get(orcRecord);
            if (realRow == null || realRow.size() <= timestampIndex) {
                return Optional.empty();
            }
            return Optional.ofNullable(realRow.get(timestampIndex));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
