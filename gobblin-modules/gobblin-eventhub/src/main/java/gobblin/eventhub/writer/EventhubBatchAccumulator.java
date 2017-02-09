/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.eventhub.writer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.Iterator;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Futures;
import com.typesafe.config.Config;

import gobblin.util.ConfigUtils;
import gobblin.writer.Batch;
import gobblin.writer.BatchAccumulator;
import gobblin.writer.RecordMetadata;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


/**
 * Eventhub Accumulator based on batch size and TTL
 */

public class EventhubBatchAccumulator extends BatchAccumulator<byte[]> {

  private Deque<EventhubBatch> dq = new ArrayDeque<>();
  private IncompleteRecordBatches incomplete = new IncompleteRecordBatches();
  private final long batchSizeLimit;
  private final long memSizeLimit;
  private final double tolerance = 0.95;
  private final long expireInMilliSecond;
  private static final Logger LOG = LoggerFactory.getLogger(EventhubBatchAccumulator.class);

  public EventhubBatchAccumulator () {
    this (1024 * 256, 3000);
  }

  public EventhubBatchAccumulator (Properties properties) {
    Config config = ConfigUtils.propertiesToConfig(properties);
    this.batchSizeLimit = ConfigUtils.getLong(config, EventhubWriterConfigurationKeys.BATCH_SIZE,
        EventhubWriterConfigurationKeys.BATCH_SIZE_DEFAULT);

    this.expireInMilliSecond = ConfigUtils.getLong(config, EventhubWriterConfigurationKeys.BATCH_TTL,
        EventhubWriterConfigurationKeys.BATCH_TTL_DEFAULT);

    this.memSizeLimit = (long) (this.tolerance * this.batchSizeLimit);
  }

  public EventhubBatchAccumulator (long batchSizeLimit, long expireInMilliSecond) {
    this.batchSizeLimit = batchSizeLimit;
    this.expireInMilliSecond = expireInMilliSecond;
    this.memSizeLimit = (long) (this.tolerance * this.batchSizeLimit);
  }

  public long getMemSizeLimit () {
    return this.memSizeLimit;
  }

  public long getExpireInMilliSecond () {
    return this.expireInMilliSecond;
  }

  /**
   * Add a data to internal dequeu data structure
   */
  public final Future<RecordMetadata> enqueue (byte[] record, WriteCallback callback) throws InterruptedException {

    synchronized (dq) {
      EventhubBatch last = dq.peekLast();
      if (last != null) {
        Future<RecordMetadata> future = last.tryAppend(record, callback);
        if (future != null)
          return future;
      }

      // Create a new batch because previous one has no space
      EventhubBatch batch = new EventhubBatch(this.memSizeLimit, this.expireInMilliSecond);
      LOG.info ("Batch " + batch.getId() + " is generated");
      Future<RecordMetadata> future = batch.tryAppend(record, callback);

      // Even single record can exceed the size limit from one batch
      // Ignore the record because Eventhub can only accept payload less than 256KB
      if (future == null) {
        LOG.debug ("Batch " + batch.getId() + " is marked as complete because it contains a huge record: "
                + Base64.getEncoder().encodeToString(record));
        future = Futures.immediateFuture(new RecordMetadata(0));
        callback.onSuccess(WriteResponse.EMPTY);
        return future;
      }

      dq.addLast(batch);
      incomplete.add(batch);
      return future;
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

    public Iterable<Batch> all() {
      synchronized (incomplete) {
        return new ArrayList (this.incomplete);
      }
    }
  }

  public Iterator<Batch<byte[]>> iterator() {
    return new EventhubBatchIterator();
  }


  /**
   * An internal iterator that will iterate all the available batches
   * This will be used by external BufferedAsyncDataWriter
   */
  private class EventhubBatchIterator implements Iterator<Batch<byte[]>> {
    public Batch<byte[]> next () {
      synchronized (dq) {
        return dq.poll();
      }
    }

    public boolean hasNext() {
      synchronized (dq) {
        if (EventhubBatchAccumulator.this.isClosed()) {
          return dq.size() > 0;
        }
        if (dq.size() > 1)
            return true;
        if (dq.size() == 0)
            return false;
        EventhubBatch first = dq.peekFirst();
        if (first.isTTLExpire()) {
            LOG.info ("Batch " + first.getId() + " is expired");
            return true;
        }

        return false;
      }
    }

    public void remove() {
      throw new UnsupportedOperationException("EventhubBatchIterator doesn't support remove operation");
    }
  }

  /**
   * This will block until all the incomplete batches are acknowledged
   */
  public void flush() {
    try {
      for (Batch batch: this.incomplete.all()) {
        batch.await();
      }
    } catch (Exception e) {
      LOG.info ("Error happens when flushing");
    }
  }

  /**
   * Once batch is acknowledged, remove it from incomplete list
   */
  public void deallocate (Batch<byte[]> batch) {
    this.incomplete.remove(batch);
  }
}
