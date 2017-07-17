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

import java.io.Closeable;
import java.io.IOException;

import gobblin.records.ControlMessageHandler;
import gobblin.stream.RecordEnvelope;


/**
 * An interface for data writers.
 *
 * @param <D> data record type
 *
 * @author Yinan Li
 */
public interface DataWriter<D> extends Closeable {

  /**
   * Write a source data record in Avro format using the given converter.
   *
   * @param record data record to write
   * @throws IOException if there is anything wrong writing the record
   */
  default void write(D record) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Commit the data written.
   *
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
   * Write the input {@link RecordEnvelope}. By default, just call {@link #write(Object)}.
   */
  default void writeEnvelope(RecordEnvelope<D> recordEnvelope) throws IOException {
    write(recordEnvelope.getRecord());
    recordEnvelope.ack();
  }

  /**
   * @return A {@link ControlMessageHandler}.
   */
  default ControlMessageHandler getMessageHandler() {
    return ControlMessageHandler.NOOP;
  }
}
