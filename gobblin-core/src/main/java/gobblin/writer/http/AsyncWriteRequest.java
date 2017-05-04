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

package gobblin.writer.http;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


/**
 * A type of write request which may batch several records at a time. It encapsulates the
 * raw request, batch level statistics, and callback of each record that will be invoked
 * on batch level callbacks.
 *
 * @param <D> type of data record
 * @param <RQ> type of raw request
 */
public class AsyncWriteRequest<D, RQ> {
  @Getter
  private int recordCount = 0;
  @Getter
  protected long bytesWritten = 0;
  @Getter @Setter
  private RQ rawRequest;

  private final List<Thunk> thunks = new ArrayList<>();

  /**
   * Callback on sending the batch successfully
   */
  public void onSuccess(final WriteResponse response) {
    for (final Thunk thunk : this.thunks) {
      thunk.callback.onSuccess(new WriteResponse() {
        @Override
        public Object getRawResponse() {
          return response.getRawResponse();
        }

        @Override
        public String getStringResponse() {
          return response.getStringResponse();
        }

        @Override
        public long bytesWritten() {
          return thunk.sizeInBytes;
        }
      });
    }
  }

  /**
   * Callback on failing to send the batch
   */
  public void onFailure(Throwable throwable) {
    for (Thunk thunk : this.thunks) {
      thunk.callback.onFailure(throwable);
    }
  }

  /**
   * Mark the record associated with this request
   */
  public void markRecord(BufferedRecord<D> record, int bytesWritten) {
    if (record.getCallback() != null) {
      thunks.add(new Thunk(record.getCallback(), bytesWritten));
    }
    recordCount++;
    this.bytesWritten += bytesWritten;
  }

  /**
   * A helper class which wraps the callback
   * It may contain more information related to each individual record
   */
  final private static class Thunk {
    final WriteCallback callback;
    final int sizeInBytes;

    Thunk(WriteCallback callback, int sizeInBytes) {
      this.callback = callback;
      this.sizeInBytes = sizeInBytes;
    }
  }
}
