/* (c) 2015 LinkedIn Corp. All rights reserved.
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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.HadoopUtils;

/**
 * An implementation of {@link DataWriter} that writes directly to HDFS in Avro format.
 *
 * @author ynli
 */
class AvroHdfsDataWriter extends FsDataWriter<GenericRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroHdfsDataWriter.class);

  private final DatumWriter<GenericRecord> datumWriter;
  private final DataFileWriter<GenericRecord> writer;

  private final Schema schema;

  // Number of records successfully written
  private final AtomicLong count = new AtomicLong(0);

  // Whether the writer has already been closed or not
  private volatile boolean closed = false;

  public enum CodecType {
    NOCOMPRESSION,
    DEFLATE,
    SNAPPY
  }

  public AvroHdfsDataWriter(State properties, String fileName, Schema schema, int numBranches, int branchId)
      throws IOException {
    super(properties, fileName, numBranches, branchId);

    String codecType = properties
        .getProp(ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_CODEC_TYPE, numBranches, branchId),
            AvroHdfsDataWriter.CodecType.DEFLATE.name());

    int bufferSize = Integer.parseInt(properties.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_BUFFER_SIZE, numBranches, branchId),
        ConfigurationKeys.DEFAULT_BUFFER_SIZE));

    int deflateLevel = Integer.parseInt(properties.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_DEFLATE_LEVEL, numBranches, branchId),
        ConfigurationKeys.DEFAULT_DEFLATE_LEVEL));

    this.schema = schema;

    this.datumWriter = new GenericDatumWriter<GenericRecord>();
    this.writer = createDatumWriter(this.stagingFile, bufferSize, CodecType.valueOf(codecType), deflateLevel);
  }

  @Override
  public void write(GenericRecord record)
      throws IOException {
    Preconditions.checkNotNull(record);

    this.writer.append(record);
    // Only increment when write is successful
    this.count.incrementAndGet();
  }

  @Override
  public void close()
      throws IOException {
    if (this.closed) {
      return;
    }

    this.writer.flush();
    this.writer.close();
    this.closed = true;
  }

  @Override
  public void commit()
      throws IOException {
    // Close the writer first if it has not been closed yet
    if (!this.closed) {
      this.close();
    }

    if (!this.fs.exists(this.stagingFile)) {
      throw new IOException(String.format("File %s does not exist", this.stagingFile));
    }

    LOG.info(String.format("Moving data from %s to %s", this.stagingFile, this.outputFile));
    // For the same reason as deleting the staging file if it already exists, deleting
    // the output file if it already exists prevents task retry from being blocked.
    if (this.fs.exists(this.outputFile)) {
      LOG.warn(String.format("Task output file %s already exists", this.outputFile));
      HadoopUtils.deletePath(this.fs, this.outputFile, false);
    }
    HadoopUtils.renamePath(this.fs, this.stagingFile, this.outputFile);
  }

  @Override
  public void cleanup()
      throws IOException {
    // Delete the staging file
    if (this.fs.exists(this.stagingFile)) {
      HadoopUtils.deletePath(this.fs, this.stagingFile, false);
    }
  }

  @Override
  public long recordsWritten() {
    return this.count.get();
  }

  @Override
  public long bytesWritten()
      throws IOException {
    if (!this.fs.exists(this.outputFile) || !this.closed) {
      return 0;
    }

    return this.fs.getFileStatus(this.outputFile).getLen();
  }

  /**
   * Create a new {@link DataFileWriter} for writing Avro records.
   *
   * @param avroFile Avro file to write to
   * @param bufferSize Buffer size
   * @param codecType Compression codec type
   * @param deflateLevel Deflate level
   * @throws IOException if there is something wrong creating a new {@link DataFileWriter}
   */
  private DataFileWriter<GenericRecord> createDatumWriter(Path avroFile, int bufferSize,
      CodecType codecType, int deflateLevel)
      throws IOException {

    if (this.fs.exists(avroFile)) {
      throw new IOException(String.format("File %s already exists", avroFile));
    }

    FSDataOutputStream outputStream = this.fs.create(avroFile, true, bufferSize);
    DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(this.datumWriter);

    // Set compression type
    switch (codecType) {
      case DEFLATE:
        writer.setCodec(CodecFactory.deflateCodec(deflateLevel));
        break;
      case SNAPPY:
        writer.setCodec(CodecFactory.snappyCodec());
        break;
      case NOCOMPRESSION:
        break;
      default:
        writer.setCodec(CodecFactory.deflateCodec(deflateLevel));
        break;
    }

    // Open the file and return the DataFileWriter
    return writer.create(this.schema, outputStream);
  }
}