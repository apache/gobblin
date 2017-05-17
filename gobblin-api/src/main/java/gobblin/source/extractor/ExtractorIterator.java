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

package gobblin.source.extractor;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;


/**
 * An {@link Iterator} consuming records from an {@link Extractor}. Handles shutdown requests transparently.
 */
@RequiredArgsConstructor
public class ExtractorIterator<D> implements Iterator<RecordEnvelope<D>> {

  private final Extractor<?, D> extractor;
  private final AtomicBoolean shutdownRequested;

  private RecordEnvelope<D> nextRecord;

  @Override
  public boolean hasNext() {
    if (this.nextRecord != null) {
      return true;
    }
    if (this.shutdownRequested.get()) {
      return false;
    }
    try {
      this.nextRecord = this.extractor.readRecordEnvelope();
      return this.nextRecord != null;
    } catch (DataRecordException | IOException exc) {
      throw new RuntimeException(exc);
    }
  }

  @Override
  public RecordEnvelope<D> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    RecordEnvelope<D> record = this.nextRecord;
    this.nextRecord = null;
    return record;
  }
}
