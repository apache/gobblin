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

package org.apache.gobblin.async;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.net.Request;


/**
 * A type of write request which may batch several records at a time. It encapsulates the
 * raw request, batch level statistics, and callback of each record
 *
 * @param <D> type of data record
 * @param <RQ> type of raw request
 */
public class AsyncRequest<D, RQ> implements Request<RQ> {
  @Getter @Setter
  private RQ rawRequest;
  protected final List<Thunk<D>> thunks = new ArrayList<>();
  private long byteSize = 0;

  /**
   * Get the total number of records processed in the request
   */
  public int getRecordCount() {
    return thunks.size();
  }

  /**
   * Get the total bytes processed in the request
   */
  public long getBytesWritten() {
    return this.byteSize;
  }

  /**
   * Get all records information in the request
   */
  public List<Thunk<D>> getThunks() {
    return ImmutableList.copyOf(thunks);
  }

  /**
   * Mark the record associated with this request
   *
   * @param record buffered record
   * @param bytesWritten bytes of the record written into the request
   */
  public void markRecord(BufferedRecord<D> record, int bytesWritten) {
    synchronized (this) {
      thunks.add(new Thunk<>(record, bytesWritten));
      byteSize += bytesWritten;
    }
  }

  /**
   * A descriptor that represents a record in the request
   */
  public static final class Thunk<D> {
    /**
     * @deprecated Use {@link #record}
     */
    @Deprecated
    public final Callback callback;

    public final int sizeInBytes;
    public final BufferedRecord<D> record;

    /**
     * @deprecated Use {@link #Thunk(BufferedRecord, int)}
     */
    @Deprecated
    Thunk(Callback callback, int sizeInBytes) {
      this.callback = callback;
      this.sizeInBytes = sizeInBytes;
      this.record = null;
    }

    Thunk(BufferedRecord<D> record, int sizeInBytes) {
      this.callback = record.getCallback();
      this.sizeInBytes = sizeInBytes;
      this.record = record;
    }
  }
}
