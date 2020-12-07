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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import lombok.Getter;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.DatePartitionType;
import org.apache.gobblin.util.ForkOperatorUtils;


/**
 * A {@link WriterPartitioner} that partitions a record based on a timestamp.
 *
 * There are two ways to partition a timestamp: (1) specify a {@link DateTimeFormat} using
 * {@link #WRITER_PARTITION_PATTERN}, e.g., 'yyyy/MM/dd/HH'; (2) specify a
 * {@link DatePartitionType} using {@link #WRITER_PARTITION_GRANULARITY}.
 *
 * A prefix and a suffix can be added to the partition, e.g., the partition path can be
 * 'prefix/2015/11/05/suffix'.
 *
 * @author Ziyang Liu
 */
public abstract class TimeBasedWriterPartitioner<D> implements WriterPartitioner<D> {

  public static final String WRITER_PARTITION_PREFIX = ConfigurationKeys.WRITER_PREFIX + ".partition.prefix";
  public static final String WRITER_PARTITION_SUFFIX = ConfigurationKeys.WRITER_PREFIX + ".partition.suffix";
  public static final String WRITER_PARTITION_PATTERN = ConfigurationKeys.WRITER_PREFIX + ".partition.pattern";
  public static final String WRITER_PARTITION_TIMEZONE = ConfigurationKeys.WRITER_PREFIX + ".partition.timezone";
  public static final String DEFAULT_WRITER_PARTITION_TIMEZONE = ConfigurationKeys.PST_TIMEZONE_NAME;
  public static final String WRITER_PARTITION_TIMEUNIT = ConfigurationKeys.WRITER_PREFIX + ".partition.timeUnit";
  public static final String DEFAULT_WRITER_PARTITION_TIMEUNIT = TimeUnit.MILLISECONDS.name();
  public static final String WRITER_PARTITION_GRANULARITY = ConfigurationKeys.WRITER_PREFIX + ".partition.granularity";
  public static final DatePartitionType DEFAULT_WRITER_PARTITION_GRANULARITY = DatePartitionType.HOUR;

  public static final String PARTITIONED_PATH = "partitionedPath";
  public static final String PREFIX = "prefix";
  public static final String SUFFIX = "suffix";

  private final String writerPartitionPrefix;
  private final String writerPartitionSuffix;
  private final DatePartitionType granularity;
  private final DateTimeZone timeZone;
  @Getter
  protected final TimeUnit timeUnit;
  private final Optional<DateTimeFormatter> timestampToPathFormatter;
  private final Schema schema;

  public TimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
    this.writerPartitionPrefix = getWriterPartitionPrefix(state, numBranches, branchId);
    this.writerPartitionSuffix = getWriterPartitionSuffix(state, numBranches, branchId);
    this.granularity = getGranularity(state, numBranches, branchId);
    this.timeZone = getTimeZone(state, numBranches, branchId);
    this.timeUnit = getTimeUnit(state, numBranches, branchId);
    this.timestampToPathFormatter = getTimestampToPathFormatter(state, numBranches, branchId);
    this.schema = getSchema();
  }

  private static String getWriterPartitionPrefix(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_PREFIX, numBranches, branchId);
    return state.getProp(propName, StringUtils.EMPTY);
  }

  private static String getWriterPartitionSuffix(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_SUFFIX, numBranches, branchId);
    return state.getProp(propName, StringUtils.EMPTY);
  }

  private static DatePartitionType getGranularity(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_GRANULARITY, numBranches, branchId);
    String granularityValue = state.getProp(propName, DEFAULT_WRITER_PARTITION_GRANULARITY.toString());
    Optional<DatePartitionType> granularity =
        Enums.getIfPresent(DatePartitionType.class, granularityValue.toUpperCase());
    Preconditions.checkState(granularity.isPresent(),
        granularityValue + " is not a valid writer partition granularity");
    return granularity.get();
  }

  private Optional<DateTimeFormatter> getTimestampToPathFormatter(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_PATTERN, numBranches, branchId);

    if (state.contains(propName)) {
      return Optional.of(DateTimeFormat.forPattern(state.getProp(propName)).withZone(this.timeZone));
    }
    return Optional.absent();
  }

  private static DateTimeZone getTimeZone(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_TIMEZONE, numBranches, branchId);
    return DateTimeZone.forID(state.getProp(propName, DEFAULT_WRITER_PARTITION_TIMEZONE));
  }

  private static TimeUnit getTimeUnit(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_TIMEUNIT, numBranches, branchId);
    return TimeUnit.valueOf(state.getProp(propName, DEFAULT_WRITER_PARTITION_TIMEUNIT).toUpperCase());
  }

  private Schema getSchema() {
    if (this.timestampToPathFormatter.isPresent()) {
      return getDateTimeFormatBasedSchema();
    }
    return getGranularityBasedSchema();
  }

  @Override
  public Schema partitionSchema() {
    return this.schema;
  }

  @SuppressWarnings("fallthrough")
  @Override
  public GenericRecord partitionForRecord(D record) {
    long timestamp = timeUnit.toMillis(getRecordTimestamp(record));
    GenericRecord partition = new GenericData.Record(this.schema);
    if (!Strings.isNullOrEmpty(this.writerPartitionPrefix)) {
      partition.put(PREFIX, this.writerPartitionPrefix);
    }
    if (!Strings.isNullOrEmpty(this.writerPartitionSuffix)) {
      partition.put(SUFFIX, this.writerPartitionSuffix);
    }

    if (this.timestampToPathFormatter.isPresent()) {
      String partitionedPath = getPartitionedPath(timestamp);
      partition.put(PARTITIONED_PATH, partitionedPath);
    } else {
      DateTime dateTime = new DateTime(timestamp, this.timeZone);
      partition.put(this.granularity.toString(), this.granularity.getField(dateTime));
    }

    return partition;
  }

  private Schema getDateTimeFormatBasedSchema() {
    FieldAssembler<Schema> assembler =
        SchemaBuilder.record("GenericRecordTimePartition").namespace("gobblin.writer.partitioner").fields();

    if (!Strings.isNullOrEmpty(this.writerPartitionPrefix)) {
      assembler = assembler.name(PREFIX).type(Schema.create(Schema.Type.STRING)).noDefault();
    }
    assembler = assembler.name(PARTITIONED_PATH).type(Schema.create(Schema.Type.STRING)).noDefault();
    if (!Strings.isNullOrEmpty(this.writerPartitionSuffix)) {
      assembler = assembler.name(SUFFIX).type(Schema.create(Schema.Type.STRING)).noDefault();
    }

    return assembler.endRecord();
  }

  @SuppressWarnings("fallthrough")
  private Schema getGranularityBasedSchema() {
    FieldAssembler<Schema> assembler =
        SchemaBuilder.record("GenericRecordTimePartition").namespace("gobblin.writer.partitioner").fields();

    // Construct the fields in reverse order
    if (!Strings.isNullOrEmpty(this.writerPartitionSuffix)) {
      assembler = assembler.name(SUFFIX).type(Schema.create(Schema.Type.STRING)).noDefault();
    }
    assembler = assembler.name(this.granularity.toString()).type(Schema.create(Schema.Type.STRING)).noDefault();

    if (!Strings.isNullOrEmpty(this.writerPartitionPrefix)) {
      assembler = assembler.name(PREFIX).type(Schema.create(Schema.Type.STRING)).noDefault();
    }

    Schema schema = assembler.endRecord();
    Collections.reverse(schema.getFields());
    return schema;
  }

  private String getPartitionedPath(long timestamp) {
    return this.timestampToPathFormatter.get().print(timestamp);
  }

  public abstract long getRecordTimestamp(D record);
}