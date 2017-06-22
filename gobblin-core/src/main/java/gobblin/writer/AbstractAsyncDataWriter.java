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
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import gobblin.async.AsyncDataDispatcher;
import gobblin.async.BufferedRecord;
import gobblin.writer.AsyncDataWriter;
import gobblin.writer.FutureWrappedWriteCallback;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


/**
 * Base class to write data asynchronously. It is an {@link AsyncDataDispatcher} on {@link BufferedRecord}, which
 * wraps a record and its callback.
 *
 * @param <D> type of record
 */
@ThreadSafe
public abstract class AbstractAsyncDataWriter<D> extends AsyncDataDispatcher<BufferedRecord<D>> implements AsyncDataWriter<D> {
  public static final int DEFAULT_BUFFER_CAPACITY = 10000;

  public AbstractAsyncDataWriter(int capacity) {
    super(capacity);
  }

  /**
   * Asynchronously write the record with a callback
   */
  @Override
  public final Future<WriteResponse> write(D record, @Nullable WriteCallback callback) {
    FutureWrappedWriteCallback wrappedWriteCallback = new FutureWrappedWriteCallback(callback);
    BufferedRecord<D> bufferedRecord = new BufferedRecord<>(record, wrappedWriteCallback);
    put(bufferedRecord);
    return wrappedWriteCallback;
  }

  @Override
  public void close()
      throws IOException {
    try {
      flush();
    } finally {
      terminate();
    }
  }

  /**
   * Wait for a buffer empty occurrence
   */
  @Override
  public void flush()
      throws IOException {
    waitForBufferEmpty();
  }
}
