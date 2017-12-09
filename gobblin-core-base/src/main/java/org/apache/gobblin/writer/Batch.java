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
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.annotation.Alpha;


/**
 * An Batch class that contains all the records pushed by {@link BatchAccumulator}.
 * Internally it maintains a callback list which contains all individual callback related to each record.
 * This class also maintains a countdown latch. This is used to block all the threads waiting on the batch
 * completion event. All the blocked threads will be resumed after {@link Batch#done()} is invoked
 *  @param <D> record data type
 */
@Alpha
public abstract class Batch<D>{

  public static final String BATCH_TTL = "writer.batch.ttl";
  public static final long   BATCH_TTL_DEFAULT = 1000; // 1 seconds
  public static final String BATCH_SIZE = "writer.batch.size";
  public static final long   BATCH_SIZE_DEFAULT = 256 * 1024; // 256KB
  public static final String BATCH_QUEUE_CAPACITY = "writer.batch.queue.capacity";
  public static final long   BATCH_QUEUE_CAPACITY_DEFAULT = 100;

  private final List<Thunk> thunks;

  private final long id;
  private long recordCount;
  private final CountDownLatch latch = new CountDownLatch(1);
  private static AtomicInteger identifier = new AtomicInteger(0);
  private static final Logger LOG = LoggerFactory.getLogger(Batch.class);

  public Batch () {
    recordCount = 0;
    thunks = new ArrayList<>();
    id = identifier.incrementAndGet();
  }

  public void done() {
    latch.countDown();
  }


  public long getId() {
    return id;
  }


  /**
   * A helper class which wraps the callback
   * It may contain more information related to each individual record
   */
  final private static class Thunk {
    final WriteCallback callback;
    final int sizeInBytes;
    public Thunk(WriteCallback callback, int sizeInBytes) {
      this.callback = callback;
      this.sizeInBytes = sizeInBytes;
    }
  }

  /**
   * After batch is sent and get acknowledged successfully, this method will be invoked
   */
  public void onSuccess (final WriteResponse response) {
    for (final Thunk thunk: this.thunks) {
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
   * When batch is sent with an error return, this method will be invoked
   */
  public void onFailure (Throwable throwable) {
    for (Thunk thunk: this.thunks) {
      thunk.callback.onFailure(throwable);
    }
  }

  /**
   * Return all the added records
   */
  public abstract List<D> getRecords();

  /**
   * Return current batch size in bytes
   */
  public abstract long getCurrentSizeInByte();

  /**
   * A method to check if the batch has the room to add a new record
   *  @param record: record needs to be added
   *  @return Indicates if this batch still have enough space to hold a new record
   */
  public abstract boolean hasRoom (D record);

  /**
   * Add a record to this batch
   * <p>
   *   Implementation of this method should always ensure the record can be added successfully
   *   The contract between {@link Batch#tryAppend(Object, WriteCallback)} and this method is this method
   *   is responsible for adding record to internal batch memory and the check for the room space is performed
   *   by {@link Batch#hasRoom(Object)}. All the potential issues for adding a record should
   *   already be resolved before this method is invoked.
   * </p>
   *
   *  @param record: record needs to be added
   */
  public abstract void append (D record);

  /**
   * Get input record size in bytes
   */
  public abstract int getRecordSizeInByte(D record) ;

  /**
   * Try to add a record to this batch
   * <p>
   *   This method first check room space if a give record can be added
   *   If there is no space for new record, a null is returned; otherwise {@link Batch#append(Object)}
   *   is invoked and {@link RecordFuture} object is returned. User can call get() method on this object
   *   which will block current thread until the batch s fully completed (sent and received acknowledgement).
   *   The future object also contains meta information where this new record is located, usually an offset inside this batch.
   * </p>
   *
   *   @param record : record needs to be added
   *   @param callback : A callback which will be invoked when the whole batch gets sent and acknowledged
   *   @return A future object which contains {@link RecordMetadata}
   */
  public Future<RecordMetadata> tryAppend(D record, WriteCallback callback) {
    if (!hasRoom(record)) {
      LOG.debug ("Cannot add " + record + " to previous batch because the batch already has " + getCurrentSizeInByte() + " bytes");
      return null;
    }

    this.append(record);
    thunks.add(new Thunk(callback, getRecordSizeInByte(record)));
    RecordFuture future = new RecordFuture(latch, recordCount);
    recordCount++;
    return future;
  }

  public void await() throws InterruptedException{
    this.latch.await();
  }

}
