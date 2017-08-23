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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.annotation.Alpha;


/**
 * An accumulator which groups multiple records into a batch
 * How batching strategy works depends on the real implementation
 * One way to do this is scanning all the internal batches through an iterator
 */
@Alpha
public abstract class BatchAccumulator<D> implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BatchAccumulator.class);

  private volatile boolean closed = false;
  private CountDownLatch closeComplete;
  private final AtomicInteger appendsInProgress;

  protected BatchAccumulator() {
    this.appendsInProgress = new AtomicInteger(0);
    this.closeComplete = new CountDownLatch(1);
  }

  /**
   * Append a record to this accumulator
   * <p>
   *   This method should never fail unless there is an exception. A future object should always be returned
   *   which can be queried to see if this record has been completed (completion means the wrapped batch has been
   *   sent and received acknowledgement and callback has been invoked). Internally it tracks how many parallel appends
   *   are in progress by incrementing appendsInProgress counter. The real append logic is inside {@link BatchAccumulator#enqueue(Object, WriteCallback)}
   * </p>
   *
   *   @param record : record needs to be added
   *   @param callback : A callback which will be invoked when the whole batch gets sent and acknowledged
   *   @return A future object which contains {@link RecordMetadata}
   */
  public final Future<RecordMetadata> append (D record, WriteCallback callback) throws InterruptedException {
    appendsInProgress.incrementAndGet();
    try {
      if (this.closed) {
        throw new RuntimeException ("Cannot append after accumulator has been closed");
      }
      return this.enqueue(record, callback);
    } finally {
      appendsInProgress.decrementAndGet();
    }
  }

  public final void waitClose() {
    try {
      this.closeComplete.await();
    } catch (InterruptedException e) {
      LOG.error ("accumulator close is interrupted");
    }

    LOG.info ("accumulator is closed");
  }

  public boolean isClosed () {
    return closed;
  }

  /**
   * Add a record to this accumulator
   * <p>
   *   This method should never fail unless there is an exception. A future object should always be returned
   *   which can be queried to see if this record has been completed (completion means the wrapped batch has been
   *   sent and received acknowledgement and callback has been invoked).
   * </p>
   *
   *   @param record : record needs to be added
   *   @param callback : A callback which will be invoked when the whole batch gets sent and acknowledged
   *   @return A future object which contains {@link RecordMetadata}
   */
  public abstract Future<RecordMetadata> enqueue (D record, WriteCallback callback) throws InterruptedException;

  /**
   * Wait until all the incomplete batches to be acknowledged
   */
  public abstract void flush ();

  /**
   * When close is invoked, all new coming records will be rejected
   * Add a busy loop here to ensure all the ongoing appends are completed
   */
  public void close () {
    closed = true;
    while (appendsInProgress.get() > 0) {
      LOG.info("Append is still going on, wait for a while");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.error("close is interrupted while appending data is in progress");
      }
    }
    this.closeComplete.countDown();
  }

  /**
   * Release some resource current batch is allocated
   */
  public abstract void deallocate (Batch<D> batch);

  public abstract Batch<D> getNextAvailableBatch();

}
