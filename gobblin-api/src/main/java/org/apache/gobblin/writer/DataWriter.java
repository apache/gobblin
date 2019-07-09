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

package org.apache.gobblin.writer;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import org.apache.gobblin.dataset.Descriptor;
import org.apache.gobblin.records.ControlMessageHandler;
import org.apache.gobblin.records.FlushControlMessageHandler;
import org.apache.gobblin.stream.RecordEnvelope;


/**
 * An interface for data writers
 *
 * <p>
 *   Generally, one work unit has a dedicated {@link DataWriter} instance, which processes only one dataset
 * </p>
 *
 * @param <D> data record type
 *
 * @author Yinan Li
 */
public interface DataWriter<D> extends Closeable, Flushable {

  /**
   * Write a data record.
   *
   * @param record data record to write
   * @throws IOException if there is anything wrong writing the record
   */
  default void write(D record) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Commit the data written.
   * This method is expected to be called at most once during the lifetime of a writer.
   * @throws IOException if there is anything wrong committing the output
   */
  public void commit()
      throws IOException;

  /**
   * Cleanup context/resources.
   *
   * @throws IOException if there is anything wrong doing cleanup.
   */
  public void cleanup()
      throws IOException;

  /**
   * Get the number of records written.
   *
   * @return number of records written
   */
  public long recordsWritten();

  /**
   * Get the number of bytes written.
   *
   * @return number of bytes written
   */
  public long bytesWritten()
      throws IOException;

  /**
   * The method should return a {@link Descriptor} that represents what the writer is writing
   *
   * <p>
   *   Note that, this information might be useless and discarded by a
   *   {@link org.apache.gobblin.publisher.DataPublisher}, which determines the final form of dataset or partition
   * </p>
   *
   * @return a {@link org.apache.gobblin.dataset.DatasetDescriptor} if it writes files of a dataset or
   *         a {@link org.apache.gobblin.dataset.PartitionDescriptor} if it writes files of a dataset partition or
   *         {@code null} if it is useless
   */
  default Descriptor getDataDescriptor() {
    return null;
  }

  /**
   * Write the input {@link RecordEnvelope}. By default, just call {@link #write(Object)}.
   * DataWriters that implement this method must acknowledge the recordEnvelope once the write has been acknowledged
   * by the destination system.
   */
  default void writeEnvelope(RecordEnvelope<D> recordEnvelope) throws IOException {
    write(recordEnvelope.getRecord());
    recordEnvelope.ack();
  }

  /**
   * Default handler calls flush on this object when a {@link org.apache.gobblin.stream.FlushControlMessage} is received
   * @return A {@link ControlMessageHandler}.
   */
  default ControlMessageHandler getMessageHandler() {
    return new FlushControlMessageHandler(this);
  }

  /**
   * Flush data written by the writer. By default, does nothing.
   * This method is expected to be called multiple times during the lifetime of a writer.
   * @throws IOException
   */
  default void flush() throws IOException {
  }
}
