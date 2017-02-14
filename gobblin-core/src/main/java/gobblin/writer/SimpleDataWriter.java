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

package gobblin.writer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


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
 * as a separator between records. If unspecified, no delimiter will be used between records.
 * </ul>
 * @author akshay@nerdwallet.com
 */
public class SimpleDataWriter extends FsDataWriter<byte[]> {

  private final Optional<Byte> recordDelimiter; // optional byte to place between each record write
  private final boolean prependSize;

  private int recordsWritten;
  private int bytesWritten;

  private final OutputStream stagingFileOutputStream;

  public SimpleDataWriter(SimpleDataWriterBuilder builder, State properties) throws IOException {
    super(builder, properties);
    String delim;
    if ((delim = properties.getProp(ConfigurationKeys.SIMPLE_WRITER_DELIMITER, null)) == null || delim.length() == 0) {
      this.recordDelimiter = Optional.absent();
    } else {
      this.recordDelimiter = Optional.of(delim.getBytes(ConfigurationKeys.DEFAULT_CHARSET_ENCODING)[0]);
    }

    this.prependSize = properties.getPropAsBoolean(ConfigurationKeys.SIMPLE_WRITER_PREPEND_SIZE, false);
    this.recordsWritten = 0;
    this.bytesWritten = 0;
    this.stagingFileOutputStream = createStagingFileOutputStream();

    setStagingFileGroup();
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
    if (this.recordDelimiter.isPresent()) {
      toWrite = Arrays.copyOf(record, record.length + 1);
      toWrite[toWrite.length - 1] = this.recordDelimiter.get();
    }
    if (this.prependSize) {
      long recordSize = toWrite.length;
      ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES);
      buf.putLong(recordSize);
      toWrite = ArrayUtils.addAll(buf.array(), toWrite);
    }
    this.stagingFileOutputStream.write(toWrite);
    this.bytesWritten += toWrite.length;
    this.recordsWritten++;
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

  @Override
  public boolean isSpeculativeAttemptSafe() {
    return this.writerAttemptIdOptional.isPresent() && this.getClass() == SimpleDataWriter.class;
  }
}
