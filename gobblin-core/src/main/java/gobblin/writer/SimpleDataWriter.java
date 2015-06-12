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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.HadoopUtils;

/**
 * An implementation of {@link DataWriter} that writes bytes directly to HDFS.
 *
 * This class accepts two new configuration parameters:
 * <ul>
 * <li>{@link ConfigurationKeys#SIMPLE_WRITER_PREPEND_SIZE} is a boolean configuration option. If true, for each record,
 * it will write out a big endian long representing the record size and then write the record. i.e. the file format
 * will be the following:
 * r := &gt;long&lt;&gt;record&lt;
 * file := empty | r file
 * <li>{@link ConfigurationKeys#SIMPLE_WRITER_DELIMITER} accepts a byte value. If specified, this byte will be used
 * as a seperator between records. If unspecified, no delimter will be used between records.
 * </ul>
 * @author akshay@nerdwallet.com
 */
public class SimpleDataWriter extends FsDataWriter<byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleDataWriter.class);

  private final OutputStream stagingFileOutputStream;
  private final Optional<Byte> recordDelimiter; // optional byte to place between each record write
  private final boolean prependSize;

  private int recordsWritten;
  private int bytesWritten;
  private boolean closed;

  public SimpleDataWriter(State properties, String fileName, int numBranches, int branchId) throws IOException {
    super(properties, fileName, numBranches, branchId);
    String delim;
    if ((delim = properties.getProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, null)) == null || delim.length() == 0) {
      this.recordDelimiter = Optional.absent();
    } else {
      this.recordDelimiter = Optional.of(delim.getBytes()[0]);
    }

    this.stagingFileOutputStream = this.fs.create(this.stagingFile, true);
    this.prependSize = properties.getPropAsBoolean(ConfigurationKeys.SIMPLE_WRITER_PREPEND_SIZE, true);
    this.recordsWritten = 0;
    this.bytesWritten = 0;
    this.closed = false;
  }
  /**
   * Write a source record to the staging file
   *
   * @param record data record to write
   * @throws java.io.IOException if there is anything wrong writing the record
   */
  @Override
  public void write(byte[] record) throws IOException {
    Preconditions.checkNotNull(record);

    byte[] toWrite = record;
    if (recordDelimiter.isPresent()) {
      toWrite = Arrays.copyOf(record, record.length + 1);
      toWrite[toWrite.length - 1] = recordDelimiter.get();
    }
    if (prependSize) {
      long recordSize = toWrite.length;
      ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES);
      buf.putLong(recordSize);
      toWrite = ArrayUtils.addAll(buf.array(), toWrite);
    }
    this.stagingFileOutputStream.write(toWrite);
    bytesWritten += toWrite.length;
    recordsWritten++;
  }

  /**
   * Commit the data written to the final output file.
   *
   * @throws java.io.IOException if there is anything wrong committing the output
   */
  @Override
  public void commit() throws IOException {
    this.close();
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

  /**
   * Cleanup context/resources.
   *
   * @throws java.io.IOException if there is anything wrong doing cleanup.
   */
  @Override
  public void cleanup() throws IOException {
    if (this.fs.exists(this.stagingFile)) {
      HadoopUtils.deletePath(this.fs, this.stagingFile, false);
    }
  }

  /**
   * Get the number of records written.
   *
   * @return number of records written
   */
  @Override
  public long recordsWritten() {
    return this.recordsWritten;
  }

  /**
   * Get the number of bytes written.
   *
   * @return number of bytes written
   */
  @Override
  public long bytesWritten() throws IOException {
    return this.bytesWritten;
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   * <p/>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws java.io.IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (stagingFileOutputStream != null && !this.closed) {
      try {
        stagingFileOutputStream.flush();
      } finally {
        stagingFileOutputStream.close();
        this.closed = true;
      }
    }
  }
}
