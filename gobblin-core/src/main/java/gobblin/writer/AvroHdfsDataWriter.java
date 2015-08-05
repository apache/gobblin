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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.WriterUtils;


/**
 * An extension to {@link FsDataWriter} that writes in Avro format in the form of {@link GenericRecord}s.
 *
 * <p>
 *   This implementation allows users to specify the {@link CodecFactory} to use through the configuration
 *   property {@link ConfigurationKeys#WRITER_CODEC_TYPE}. By default, the deflate codec is used.
 * </p>
 *
 * @author ynli
 */
class AvroHdfsDataWriter extends FsDataWriter<GenericRecord> {

  private final Schema schema;
  private final DatumWriter<GenericRecord> datumWriter;
  private final DataFileWriter<GenericRecord> writer;

  // Number of records successfully written
  protected final AtomicLong count = new AtomicLong(0);

  public AvroHdfsDataWriter(State properties, String fileName, Schema schema, int numBranches, int branchId)
      throws IOException {
    super(properties, fileName, numBranches, branchId);

    CodecFactory codecFactory =
        WriterUtils.getCodecFactory(Optional.fromNullable(properties.getProp(
            ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_CODEC_TYPE, numBranches, branchId))),
            Optional.fromNullable(properties.getProp(ForkOperatorUtils
                .getPropertyNameForBranch(ConfigurationKeys.WRITER_DEFLATE_LEVEL, numBranches, branchId))));

    this.schema = schema;
    this.datumWriter = new GenericDatumWriter<GenericRecord>();
    this.writer = this.closer.register(createDataFileWriter(codecFactory));
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  @Override
  public void write(GenericRecord record) throws IOException {
    Preconditions.checkNotNull(record);

    this.writer.append(record);
    // Only increment when write is successful
    this.count.incrementAndGet();
  }

  @Override
  public long recordsWritten() {
    return this.count.get();
  }

  @Override
  public long bytesWritten() throws IOException {
    if (!this.fs.exists(this.outputFile)) {
      return 0;
    }

    return this.fs.getFileStatus(this.outputFile).getLen();
  }

  /**
   * Create a new {@link DataFileWriter} for writing Avro records.
   *
   * @param codecFactory a {@link CodecFactory} object for building the compression codec
   * @throws IOException if there is something wrong creating a new {@link DataFileWriter}
   */
  private DataFileWriter<GenericRecord> createDataFileWriter(CodecFactory codecFactory) throws IOException {
    DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(this.datumWriter);
    writer.setCodec(codecFactory);

    // Open the file and return the DataFileWriter
    return writer.create(this.schema, this.stagingFileOutputStream);
  }
}
