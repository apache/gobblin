/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import com.google.common.io.Closer;

import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.util.Decorator;
import gobblin.util.limiter.Limiter;


/**
 * A decorator class for {@link Extractor} that uses a {@link Limiter} on data record extraction.
 *
 * <p>
 *   The fact that the {@link Limiter} is passed in as a parameter to the constructor
 *   {@link LimitingExtractorDecorator#LimitingExtractorDecorator(Extractor, Limiter)}
 *   means multiple {@link LimitingExtractorDecorator}s can share a single {@link Limiter}
 *   or each individual {@link LimitingExtractorDecorator} has its own {@link Limiter}.
 *   The first case is useful for throttling at above the task level, e.g., at the job level.
 * </p>
 *
 * @param <S> output schema type
 * @param <D> output record type
 *
 * @author ynli
 */
public class LimitingExtractorDecorator<S, D> implements Extractor<S, D>, Decorator {

  private final Extractor<S, D> extractor;
  private final Limiter limiter;

  public LimitingExtractorDecorator(Extractor<S, D> extractor, Limiter limiter) {
    this.extractor = extractor;
    this.limiter = limiter;
    this.limiter.start();
  }

  @Override
  public Object getDecoratedObject() {
    return this.extractor;
  }

  @Override
  public S getSchema()
      throws IOException {
    return this.extractor.getSchema();
  }

  @Override
  public D readRecord(@Deprecated D reuse)
      throws DataRecordException, IOException {
    Closer closer = Closer.create();
    try {
      if (closer.register(this.limiter.acquirePermits(1)) != null) {
        return this.extractor.readRecord(reuse);
      }
      return null;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while trying to acquire the next permit", ie);
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
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
      this.limiter.stop();
    }
  }
}
