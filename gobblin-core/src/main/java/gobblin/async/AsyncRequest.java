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

package gobblin.async;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import gobblin.net.Request;


/**
 * A type of write request which may batch several records at a time. It encapsulates the
 * raw request, batch level statistics, and callback of each record
 *
 * @param <D> type of data record
 * @param <RQ> type of raw request
 */
public class AsyncRequest<D, RQ> implements Request<RQ> {
  @Getter
  private int recordCount = 0;
  @Getter
  protected long bytesWritten = 0;
  @Getter @Setter
  private RQ rawRequest;

  @Getter
  private final List<Thunk> thunks = new ArrayList<>();

  /**
   * Mark the record associated with this request
   *
   * @param record buffered record
   * @param bytesWritten bytes of the record written into the request
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
  public static final class Thunk {
    public final Callback callback;
    public final int sizeInBytes;

    Thunk(Callback callback, int sizeInBytes) {
      this.callback = callback;
      this.sizeInBytes = sizeInBytes;
    }
  }
}
