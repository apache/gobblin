/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.io.IOException;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;


/**
 * A decorator class for {@link Extractor} that adds throttling on data record extraction.
 * The actual throttling logic is implemented by a {@link Throttler}.
 *
 * <p>
 *   The fact that the {@link Throttler} is passed in as a parameter to the constructor
 *   {@link ThrottlingExtractorDecorator#ThrottlingExtractorDecorator(Extractor, Throttler)}
 *   means multiple {@link ThrottlingExtractorDecorator}s can share a single {@link Throttler}
 *   or each individual {@link ThrottlingExtractorDecorator} has its own {@link Throttler}.
 *   The first case is useful for throttling at above the task level, e.g., at the job level.
 * </p>
 *
 * @param <S> output schema type
 * @param <D> output record type
 *
 * @author ynli
 */
public class ThrottlingExtractorDecorator<S, D> implements Extractor<S, D> {

  private final Extractor<S, D> extractor;
  private final Throttler throttler;

  public ThrottlingExtractorDecorator(Extractor<S, D> extractor, Throttler throttler) {
    this.extractor = extractor;
    this.throttler = throttler;
    this.throttler.start();
  }

  @Override
  public S getSchema()
      throws IOException {
    return this.extractor.getSchema();
  }

  @Override
  public D readRecord(@Deprecated D reuse)
      throws DataRecordException, IOException {
    try {
      if (this.throttler.waitForNextPermit()) {
        return this.extractor.readRecord(reuse);
      }
      return null;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for the next permit from the throttler", ie);
    }
  }

  @Override
  public long getExpectedRecordCount() {
    return this.extractor.getExpectedRecordCount();
  }

  @Override
  public long getHighWatermark() {
    return this.extractor.getHighWatermark();
  }

  @Override
  public void close()
      throws IOException {
    try {
      this.extractor.close();
    } finally {
      this.throttler.stop();
    }
  }
}
