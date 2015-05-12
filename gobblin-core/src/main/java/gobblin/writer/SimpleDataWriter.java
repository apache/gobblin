/* (c) 2015 NerdWallet All rights reserved.
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

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ForkOperatorUtils;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * An implementation of {@link DataWriter} that writes bytes directly to HDFS.
 *
 * @author akshay@nerdwallet.com
 */
public class SimpleDataWriter extends BaseDataWriter<byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleDataWriter.class);

  private final FSDataOutputStream outputStream; // the output stream to the staging file
  private final Byte recordDelimiter; // optional byte to place between each record write
  private final boolean prependSize;

  private int recordsWritten;
  private int bytesWritten;
  private boolean closed;

  public SimpleDataWriter(State properties, String fileName, int numBranches, int branchId) throws IOException {
    super(properties, fileName, numBranches, branchId);
    String delim;
    if ((delim = properties.getProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, null)) == null || delim.length() == 0) {
      this.recordDelimiter = null;
    } else {
      this.recordDelimiter = delim.getBytes()[0];
    }

    this.outputStream = this.fs.create(this.stagingFile, true);
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
    byte[] toWrite = record;
    Preconditions.checkNotNull(record);
    if (recordDelimiter != null) {
      toWrite = Arrays.copyOf(record, record.length + 1);
      toWrite[toWrite.length - 1] = recordDelimiter;
    }
    if (prependSize) {
      Long recordSize = new Long(toWrite.length);
      ByteBuffer buf = ByteBuffer.allocate(Long.SIZE / 8);
      buf.putLong(recordSize);
      toWrite = ArrayUtils.addAll(buf.array(), toWrite);
    }
    this.outputStream.write(toWrite);
    bytesWritten += (toWrite.length);
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
      this.deletePath(this.outputFile, false);
    }
    this.fs.rename(this.stagingFile, this.outputFile);
  }

  /**
   * Cleanup context/resources.
   *
   * @throws java.io.IOException if there is anything wrong doing cleanup.
   */
  @Override
  public void cleanup() throws IOException {
    if (this.fs.exists(this.stagingFile)) {
      this.deletePath(this.stagingFile, false);
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
   * <p/>
   * <p>
   * This method should ONLY be called after {@link gobblin.writer.DataWriter#commit()}
   * is called.
   * </p>
   *
   * @return number of bytes written
   */
  @Override
  public long bytesWritten() throws IOException {
    if (this.closed) {
      return this.bytesWritten;
    } else {
      return 0;
    }
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
    if (outputStream != null && !this.closed) {
      outputStream.flush();
      outputStream.close();
      this.closed = true;
    }
  }
}
