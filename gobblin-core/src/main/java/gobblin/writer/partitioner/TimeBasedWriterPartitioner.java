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

import java.util.Collections;

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

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;


/**
 * A {@link WriterPartitioner} that partitions a record based on a timestamp.
 *
 * There are two ways to partition a timestamp: (1) specify a {@link DateTimeFormat} using
 * {@link #WRITER_PARTITION_PATTERN}, e.g., 'yyyy/MM/dd/HH'; (2) specify a
 * {@link TimeBasedWriterPartitioner.Granularity} using {@link #WRITER_PARTITION_GRANULARITY}.
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
  public static final String WRITER_PARTITION_GRANULARITY = ConfigurationKeys.WRITER_PREFIX + ".partition.granularity";
  public static final Granularity DEFAULT_WRITER_PARTITION_GRANULARITY = Granularity.HOUR;

  public static final String PARTITIONED_PATH = "partitionedPath";
  public static final String PREFIX = "prefix";
  public static final String SUFFIX = "suffix";

  public enum Granularity {
    YEAR,
    MONTH,
    DAY,
    HOUR,
    MINUTE;
  }

  private final String writerPartitionPrefix;
  private final String writerPartitionSuffix;
  private final Granularity granularity;
  private final DateTimeZone timeZone;
  private final Optional<DateTimeFormatter> timestampToPathFormatter;
  private final Schema schema;

  public TimeBasedWriterPartitioner(State state, int numBranches, int branchId) {
    this.writerPartitionPrefix = getWriterPartitionPrefix(state, numBranches, branchId);
    this.writerPartitionSuffix = getWriterPartitionSuffix(state, numBranches, branchId);
    this.granularity = getGranularity(state, numBranches, branchId);
    this.timeZone = getTimeZone(state, numBranches, branchId);
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

  private static Granularity getGranularity(State state, int numBranches, int branchId) {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(WRITER_PARTITION_GRANULARITY, numBranches, branchId);
    String granularityValue = state.getProp(propName, DEFAULT_WRITER_PARTITION_GRANULARITY.toString());
    Optional<Granularity> granularity = Enums.getIfPresent(Granularity.class, granularityValue.toUpperCase());
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
    long timestamp = getRecordTimestamp(record);
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
      switch (this.granularity) {
        case MINUTE:
          partition.put(Granularity.MINUTE.toString(), dateTime.getMinuteOfHour());
        case HOUR:
          partition.put(Granularity.HOUR.toString(), dateTime.getHourOfDay());
        case DAY:
          partition.put(Granularity.DAY.toString(), dateTime.getDayOfMonth());
        case MONTH:
          partition.put(Granularity.MONTH.toString(), dateTime.getMonthOfYear());
        case YEAR:
          partition.put(Granularity.YEAR.toString(), dateTime.getYear());
        default:
          break;
      }
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

    switch (this.granularity) {
      case MINUTE:
        assembler = assembler.name(Granularity.MINUTE.toString()).type(Schema.create(Schema.Type.STRING)).noDefault();
      case HOUR:
        assembler = assembler.name(Granularity.HOUR.toString()).type(Schema.create(Schema.Type.STRING)).noDefault();
      case DAY:
        assembler = assembler.name(Granularity.DAY.toString()).type(Schema.create(Schema.Type.STRING)).noDefault();
      case MONTH:
        assembler = assembler.name(Granularity.MONTH.toString()).type(Schema.create(Schema.Type.STRING)).noDefault();
      case YEAR:
        assembler = assembler.name(Granularity.YEAR.toString()).type(Schema.create(Schema.Type.STRING)).noDefault();
      default:
        break;
    }

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
