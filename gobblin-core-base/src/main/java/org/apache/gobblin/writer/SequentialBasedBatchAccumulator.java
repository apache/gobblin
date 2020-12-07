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

import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.typesafe.config.Config;

import org.apache.gobblin.util.ConfigUtils;


/**
 * Sequential and TTL based accumulator
 * A producer can add a record to this accumulator. It generates a batch on the first record arrival. All subsequent records
 * are added to the same batch until a batch size limit is reached. {@link BufferedAsyncDataWriter} keeps iterating available
 * batches from this accumulator, all completed batches (full sized) will be popped out one by one but an incomplete batch
 * keeps in the deque until a TTL is expired.
 */

public class SequentialBasedBatchAccumulator<D> extends BatchAccumulator<D> {

  private static final LargeMessagePolicy DEFAULT_LARGE_MESSAGE_POLICY = LargeMessagePolicy.FAIL;
  private Deque<BytesBoundedBatch<D>> dq = new LinkedList<>();
  private IncompleteRecordBatches incomplete = new IncompleteRecordBatches();
  private final long batchSizeLimit;
  private final long memSizeLimit;
  private final double tolerance = 0.95;
  private final long expireInMilliSecond;
  private final LargeMessagePolicy largeMessagePolicy;
  private static final Logger LOG = LoggerFactory.getLogger(SequentialBasedBatchAccumulator.class);

  private final ReentrantLock dqLock = new ReentrantLock();
  private final Condition notEmpty = dqLock.newCondition();
  private final Condition notFull = dqLock.newCondition();
  private final long capacity;

  public SequentialBasedBatchAccumulator() {
    this (1024 * 256, 1000, 100);
  }

  public SequentialBasedBatchAccumulator(Properties properties) {
    this(ConfigUtils.propertiesToConfig(properties));
  }

  public SequentialBasedBatchAccumulator(Config config) {
    this(ConfigUtils.getLong(config, Batch.BATCH_SIZE,
            Batch.BATCH_SIZE_DEFAULT),
        ConfigUtils.getLong(config, Batch.BATCH_TTL,
            Batch.BATCH_TTL_DEFAULT),
        ConfigUtils.getLong(config, Batch.BATCH_QUEUE_CAPACITY,
            Batch.BATCH_QUEUE_CAPACITY_DEFAULT));
  }

  public SequentialBasedBatchAccumulator(long batchSizeLimit, long expireInMilliSecond, long capacity) {
    this(batchSizeLimit, expireInMilliSecond, capacity, DEFAULT_LARGE_MESSAGE_POLICY);
  }

  public SequentialBasedBatchAccumulator(long batchSizeLimit,
      long expireInMilliSecond,
      long capacity,
      LargeMessagePolicy largeMessagePolicy) {
    this.batchSizeLimit = batchSizeLimit;
    this.expireInMilliSecond = expireInMilliSecond;
    this.capacity = capacity;
    this.memSizeLimit = (long) (this.tolerance * this.batchSizeLimit);
    this.largeMessagePolicy = largeMessagePolicy;
  }

  public long getNumOfBatches () {
    this.dqLock.lock();
    try {
      return this.dq.size();
    } finally {
      this.dqLock.unlock();
    }
  }

  /**
   * Add a data to internal deque data structure
   */
  public final Future<RecordMetadata> enqueue (D record, WriteCallback callback) throws InterruptedException {
    final ReentrantLock lock = this.dqLock;
    lock.lock();
    try {
      BytesBoundedBatch last = dq.peekLast();
      if (last != null) {
        Future<RecordMetadata> future = null;
        try {
          future = last.tryAppend(record, callback, this.largeMessagePolicy);
        } catch (RecordTooLargeException e) {
          // Ok if the record was too large for the current batch
        }
        if (future != null) {
          return future;
        }
      }

      // Create a new batch because previous one has no space
      BytesBoundedBatch batch = new BytesBoundedBatch(this.memSizeLimit, this.expireInMilliSecond);
      LOG.debug("Batch " + batch.getId() + " is generated");
      Future<RecordMetadata> future = null;
      try {
        future = batch.tryAppend(record, callback, this.largeMessagePolicy);
      } catch (RecordTooLargeException e) {
        // If a new batch also wasn't able to accomodate the new message
        throw new RuntimeException("Failed due to a message that was too large", e);
      }

      // The future might be null, since the largeMessagePolicy might be set to DROP
      if (future == null) {
        assert largeMessagePolicy.equals(LargeMessagePolicy.DROP);
        LOG.error("Batch " + batch.getId() + " is silently marked as complete, dropping a huge record: "
                + record);
        future = Futures.immediateFuture(new RecordMetadata(0));
        callback.onSuccess(WriteResponse.EMPTY);
        return future;
      }

      // if queue is full, we should not add more
      while (dq.size() >= this.capacity) {
        LOG.debug("Accumulator size {} is greater than capacity {}, waiting", dq.size(), this.capacity);
        this.notFull.await();
      }
      dq.addLast(batch);
      incomplete.add(batch);
      this.notEmpty.signal();
      return future;

    } finally {
      lock.unlock();
    }
  }

  /**
   * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
   * This is mainly used for flush operation so that all the batches waiting in
   * the incomplete set will be blocked
   */
  private final static class IncompleteRecordBatches {
    private final Set<Batch> incomplete;

    public IncompleteRecordBatches() {
      this.incomplete = new HashSet<>();
    }

    public void add(Batch batch) {
      synchronized (incomplete) {
        this.incomplete.add(batch);
      }
    }

    public void remove(Batch batch) {
      synchronized (incomplete) {
        boolean removed = this.incomplete.remove(batch);
        if (!removed)
          throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
      }
    }

    public ArrayList<Batch> all() {
      synchronized (incomplete) {
        return new ArrayList (this.incomplete);
      }
    }
  }


  /**
   * If accumulator has been closed,  below actions are performed:
   *    1) remove and return the first batch if available.
   *    2) return null if queue is empty.
   * If accumulator has not been closed, below actions are performed:
   *    1) if queue.size == 0, block current thread until more batches are available or accumulator is closed.
   *    2) if queue size == 1, remove and return the first batch if TTL has expired, else return null.
   *    3) if queue size > 1, remove and return the first batch element.
   */
  public Batch<D> getNextAvailableBatch () {
    final ReentrantLock lock = SequentialBasedBatchAccumulator.this.dqLock;
    try {
      lock.lock();
      if (SequentialBasedBatchAccumulator.this.isClosed()) {
        return dq.poll();
      } else {
          while (dq.size() == 0) {
            LOG.debug ("ready to sleep because of queue is empty");
            SequentialBasedBatchAccumulator.this.notEmpty.await();
            if (SequentialBasedBatchAccumulator.this.isClosed()) {
              return dq.poll();
            }
          }

          if (dq.size() > 1) {
            BytesBoundedBatch candidate = dq.poll();
            SequentialBasedBatchAccumulator.this.notFull.signal();
            LOG.debug ("retrieve batch " + candidate.getId());
            return candidate;
          }

          if (dq.size() == 1) {
            if (dq.peekFirst().isTTLExpire()) {
              LOG.debug ("Batch " + dq.peekFirst().getId() + " is expired");
              BytesBoundedBatch candidate = dq.poll();
              SequentialBasedBatchAccumulator.this.notFull.signal();
              return candidate;
            } else {
              return null;
            }
          } else {
            throw new RuntimeException("Should never get to here");
          }
      }

    } catch (InterruptedException e) {
      LOG.error("Wait for next batch is interrupted. " + e.toString());
    } finally {
      lock.unlock();
    }

    return null;
  }

  public void close() {
    super.close();
    this.dqLock.lock();
    try {
      this.notEmpty.signal();
    } finally {
      this.dqLock.unlock();
    }
  }

  /**
   * This will block until all the incomplete batches are acknowledged
   */
  public void flush() {
    try {
      ArrayList<Batch> batches = this.incomplete.all();
      int numOutstandingRecords = 0;
      for (Batch batch: batches) {
        numOutstandingRecords += batch.getRecords().size();
      }
      LOG.debug ("Flush called on {} batches with {} records total", batches.size(), numOutstandingRecords);
      for (Batch batch: batches) {
        batch.await();
      }
    } catch (Exception e) {
      LOG.error ("Error happened while flushing batches");
    }
  }

  /**
   * Once batch is acknowledged, remove it from incomplete list
   */
  public void deallocate (Batch<D> batch) {
    this.incomplete.remove(batch);
  }
}
