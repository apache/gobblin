/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.writer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.AvroUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;


/**
 * Implementation of {@link DataWriter} that writes data into a date-partitioned directory structure based on the value
 * of a specific field in each Avro record.
 *
 * <p>
 *
 * The writer takes in each Avro record, extracts out a specific field, and based on the the value of the column it
 * writes the data into a specific file. The column is assumed to be of type {@link Long}, and the value is treated as
 * a timestamp. By default, the timestamp is converted into a directory of the form [yyyy]/[MM][dd]/. However, the
 * actual pattern used to construct the path is specified by {@link ConfigurationKeys#WRITER_PARTITION_PATTERN}. This
 * class uses Joda's {@link DateTimeFormatter} to parse a timestamp, and convert it to a {@link String}. The
 * {@link String} is then converted to a Path, which specifies where the record will be written.
 *
 * <p>
 *
 * By default, The complete output path for a directory is "/datasetName/daily/[yyyy]/[MM]/[dd]/fileName.avro". The
 * datasetName is specified by the configuration key {@link ConfigurationKeys#WRITER_FILE_PATH}. The writer uses the
 * configuration key {@link ConfigurationKeys#WRITER_PARTITION_COLUMN_NAME} to determine the name of the column to
 * partition by. The "daily" portion of the path is also configurable, and can be changed by setting
 * {@link ConfigurationKeys#DEFAULT_WRITER_PARTITION_LEVEL}.
 *
 * <p>
 *
 * The class works by maintaining a {@link Map} of {@link Path}s to {@link DataWriter}s called {@link #pathToWriterMap}.
 * The {@link AvroHdfsTimePartitionedWriter#write(GenericRecord)} takes each record, extracts out the the value of the
 * field specified by {@link ConfigurationKeys#WRITER_PARTITION_COLUMN_NAME}, and constructs a {@link Path} based on the
 * field value. If a {@link DataWriter} already exists for the path, the record is simply written to the existing
 * {@link DataWriter}. If one does not exist, then a new {@link DataWriter} is created, added to the Map, and then the
 * input record is written to the writer.
 *
 * <p>
 *
 * By using the above approach methods such as {@link DataWriter#commit()}, {@link DataWriter#close()}, etc. are simple.
 * The implementation is to simply iterate over the values of {@link #pathToWriterMap} and call the corresponding method
 * on each {@link DataWriter}.
 */
public class AvroHdfsTimePartitionedWriter implements DataWriter<GenericRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroHdfsTimePartitionedWriter.class);

  /**
   * This is the base file path that all data will be written to. By default, data will be written to
   * /datasetName/daily/[yyyy]/[MM]/[dd]/.
   */
  private final Path datasetName;

  /**
   * The names of the columns that the writer will use to partition the data.
   * The writer will try the columns one by one in the order of their positions in the list,
   * until it finds a column that is present in the data record.
   *
   * If partition columns are not specified, or no column in the list is present in the data record,
   * the writer will partition the data based on the current time.
   */
  private final Optional<List<String>> partitionColumns;

  /**
   * The name that separates the {@link #datasetName} from the path created by the {@link #timestampToPathFormatter}.
   * The default value is specified by {@link ConfigurationKeys#DEFAULT_WRITER_PARTITION_LEVEL}.
   */
  private final String partitionLevel;

  /**
   * This {@link DateTimeFormatter} controls how the date-partitioned directories will be created. The default pattern
   * used is specified by {@link ConfigurationKeys#DEFAULT_WRITER_PARTITION_PATTERN}. The
   * {@link DateTimeFormatter#print(long)} method is used to convert a timestamp to a {@link String}.
   */
  private final DateTimeFormatter timestampToPathFormatter;

  /**
   * Maps a {@link Path} to the the {@link DataWriter} that is writing data to the Path.
   */
  protected final Map<Path, DataWriter<GenericRecord>> pathToWriterMap = Maps.newHashMap();

  // Variables needed to build DataWriters
  private final Destination destination;
  protected final String writerId;
  private final Schema schema;
  protected final WriterOutputFormat writerOutputFormat;
  protected final State properties;
  protected final int numBranches;
  protected final int branch;

  private long earliestTimestampWritten = Long.MAX_VALUE;
  private double totalTimestampWritten = 0.0;

  public AvroHdfsTimePartitionedWriter(Destination destination, String writerId, Schema schema,
      WriterOutputFormat writerOutputFormat, int numBranches, int branch) {

    // Confirm that all input parameters are not null
    Preconditions.checkNotNull(destination);
    Preconditions.checkNotNull(writerId);
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(writerOutputFormat);
    Preconditions.checkNotNull(numBranches);
    Preconditions.checkNotNull(branch);
    Preconditions.checkNotNull(destination.getProperties());

    this.destination = destination;
    this.writerId = writerId;
    this.schema = schema;
    this.writerOutputFormat = writerOutputFormat;
    this.numBranches = numBranches;
    this.branch = branch;
    this.properties = destination.getProperties();
    this.datasetName = WriterUtils.getWriterFilePath(this.properties, numBranches, branch);

    // Initialize the partitionLevel
    this.partitionLevel =
        this.properties.getProp(getWriterPartitionLevel(), ConfigurationKeys.DEFAULT_WRITER_PARTITION_LEVEL);

    // Initialize the timestampToPathFormatter
    this.timestampToPathFormatter = DateTimeFormat
        .forPattern(
            this.properties.getProp(getWriterPartitionPattern(), ConfigurationKeys.DEFAULT_WRITER_PARTITION_PATTERN))
        .withZone(DateTimeZone.forID(this.properties.getProp(ConfigurationKeys.WRITER_PARTITION_TIMEZONE,
            ConfigurationKeys.DEFAULT_WRITER_PARTITION_TIMEZONE)));

    this.partitionColumns = getWriterPartitionColumns();
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    write(record, getRecordTimestamp(record));
  }

  protected long getRecordTimestamp(GenericRecord record) {
    return getRecordTimestamp(getWriterPartitionColumnValue(record));
  }

  /**
   *  Check if the partition column value is present and is a Long object. Otherwise, use current system time.
   */
  protected long getRecordTimestamp(Optional<Object> writerPartitionColumnValue) {
    return writerPartitionColumnValue.orNull() instanceof Long ? (Long) writerPartitionColumnValue.get()
        : System.currentTimeMillis();
  }

  /**
   * Retrieve the value of the partition column field specified by this.partitionColumns
   */
  protected Optional<Object> getWriterPartitionColumnValue(GenericRecord record) {
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

  protected void write(GenericRecord record, long recordTimestamp) throws IOException {
    Path writerOutputPath = getPathForColumnValue(recordTimestamp);

    // If the path is not in pathToWriterMap, construct a new DataWriter, add it to the map, and write the record
    // If the path is in pathToWriterMap simply retrieve the writer, and write the record
    if (!this.pathToWriterMap.containsKey(writerOutputPath)) {

      LOG.info("Creating a new DataWriter for path: " + writerOutputPath);

      DataWriter<GenericRecord> avroHdfsDataWriter = createAvroHdfsDataWriterForPath(writerOutputPath);

      this.pathToWriterMap.put(writerOutputPath, avroHdfsDataWriter);
    }

    this.pathToWriterMap.get(writerOutputPath).write(record);
    this.earliestTimestampWritten = Math.min(this.earliestTimestampWritten, recordTimestamp);
    this.totalTimestampWritten += recordTimestamp;
  }

  @Override
  public void commit() throws IOException {
    boolean commitFailed = false;
    for (Entry<Path, DataWriter<GenericRecord>> entry : this.pathToWriterMap.entrySet()) {
      try {
        entry.getValue().commit();
      } catch (IOException e) {
        commitFailed = true;
        LOG.error("Failed to close writer for path: " + entry.getKey(), e);
      }
    }

    if (commitFailed) {
      throw new IOException("Failed to commit all data for all writers");
    }
  }

  @Override
  public void cleanup() throws IOException {
    boolean cleanupFailed = false;
    for (Entry<Path, DataWriter<GenericRecord>> entry : this.pathToWriterMap.entrySet()) {
      try {
        entry.getValue().cleanup();
      } catch (IOException e) {
        cleanupFailed = true;
        LOG.error("Failed to cleanup writer for path: " + entry.getKey(), e);
      }
    }

    if (cleanupFailed) {
      throw new IOException("Failed to cleanup all writers");
    }
  }

  @Override
  public long recordsWritten() {
    long recordsWritten = 0;
    for (DataWriter<GenericRecord> dataWriter : this.pathToWriterMap.values()) {
      recordsWritten += dataWriter.recordsWritten();
    }
    return recordsWritten;
  }

  @Override
  public long bytesWritten() throws IOException {
    long bytesWritten = 0;
    boolean getBytesWritten = false;

    for (Entry<Path, DataWriter<GenericRecord>> entry : this.pathToWriterMap.entrySet()) {
      try {
        bytesWritten += entry.getValue().bytesWritten();
      } catch (IOException e) {
        getBytesWritten = true;
        LOG.error("Failed to get bytes written for path: " + entry.getKey(), e);
      }
    }

    if (getBytesWritten) {
      throw new IOException("Failed to get bytes written for all writers");
    }
    return bytesWritten;
  }

  @Override
  public void close() throws IOException {

    // Add records written and bytes written to task state
    this.properties.setProp(ConfigurationKeys.WRITER_RECORDS_WRITTEN, recordsWritten());
    this.properties.setProp(ConfigurationKeys.WRITER_BYTES_WRITTEN, bytesWritten());

    // Add earliest timestamp and average timestamp to task state
    this.properties.setProp(ConfigurationKeys.WRITER_EARLIEST_TIMESTAMP, this.earliestTimestampWritten);
    this.properties.setProp(ConfigurationKeys.WRITER_AVERAGE_TIMESTAMP,
        (long) (this.totalTimestampWritten / recordsWritten()));

    // Close all writers
    boolean closeFailed = false;
    for (Entry<Path, DataWriter<GenericRecord>> entry : this.pathToWriterMap.entrySet()) {
      try {
        entry.getValue().close();
      } catch (IOException e) {
        closeFailed = true;
        LOG.error("Failed to close writer for path: " + entry.getKey(), e);
      }
    }

    if (closeFailed) {
      throw new IOException("Failed to close all writers");
    }
  }

  /**
   * Helper method to create a new {@link DataWriter} that will write to a specific {@link Path}. The new
   * {@link DataWriter} will have all the same properties use to construct {@link AvroHdfsTimePartitionedWriter}, except
   * the {@link ConfigurationKeys#WRITER_FILE_PATH} will be different.
   * @param path that the new {@link DataWriter} will write to.
   * @return a new {@link DataWriter} configured to write to the specified path.
   * @throws IOException if there is an problem creating the new {@link DataWriter}.
   */
  private DataWriter<GenericRecord> createAvroHdfsDataWriterForPath(Path path) throws IOException {

    // Create a copy of the properties object
    State state = new State();
    for (String key : this.properties.getPropertyNames()) {
      state.setProp(key, this.properties.getProp(key));
    }

    // Set the output path that the DataWriter will write to
    state.setProp(getWriterFilePath(), path.toString());

    return new AvroDataWriterBuilder().writeTo(Destination.of(this.destination.getType(), state))
        .writeInFormat(this.writerOutputFormat).withWriterId(this.writerId).withSchema(this.schema)
        .forBranch(this.branch).build();
  }

  /**
   * Given a timestamp of type long, convert the timestamp to a {@link Path} using the {@link #timestampToPathFormatter}.
   * @param timestamp is the timestamp that needs to be converted to a path.
   * @return a {@link Path} based on the value of the timestamp.
   */
  private Path getPathForColumnValue(long timestamp) {
    return new Path(this.datasetName, partitionLevel + Path.SEPARATOR + timestampToPathFormatter.print(timestamp));
  }

  /**
   * Helper method to get the time partition columns.
   */
  private Optional<List<String>> getWriterPartitionColumns() {
    String propName = ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_COLUMNS,
        this.numBranches, this.branch);
    if (this.properties.contains(propName)) {
      return Optional.of(this.properties.getPropAsList(propName));
    } else {
      @SuppressWarnings("deprecation")
      String deprecatedPropName = ForkOperatorUtils
          .getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_COLUMN_NAME, this.numBranches, this.branch);
      if (this.properties.contains(deprecatedPropName)) {
        return Optional.of((List<String>) ImmutableList.of(this.properties.getProp(deprecatedPropName)));
      }
    }
    return Optional.absent();
  }

  /**
   * Helper method to get the branched configuration key for {@link ConfigurationKeys#WRITER_FILE_PATH}.
   */
  private String getWriterFilePath() {
    return ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_PATH, this.numBranches,
        this.branch);
  }

  /**
   * Helper method to get the branched configuration key for {@link ConfigurationKeys#WRITER_PARTITION_LEVEL}.
   */
  private String getWriterPartitionLevel() {
    return ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_LEVEL, this.numBranches,
        this.branch);
  }

  /**
   * Helper method to get the branched configuration key for {@link ConfigurationKeys#WRITER_PARTITION_PATTERN}.
   */
  private String getWriterPartitionPattern() {
    return ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_PARTITION_PATTERN, this.numBranches,
        this.branch);
  }
}
