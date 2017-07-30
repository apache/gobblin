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

package gobblin.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import gobblin.annotation.Alpha;

/**
 * An async data writer which can achieve the buffering and batching capability.
 * Internally it uses {@link BatchAccumulator} to accumulate input records. The records
 * will be converted to batches according to the accumulator implementation. The {@link RecordProcessor}
 * is responsible to iterate all available batches and write each batch via a {@link BatchAsyncDataWriter}
 *
 * @param <D> data record type
 */
@Alpha
public abstract class BufferedAsyncDataWriter<D> implements AsyncDataWriter<D> {

  private RecordProcessor<D> processor;
  private BatchAccumulator<D> accumulator;
  private ExecutorService service;
  private volatile boolean running;
  private final long startTime;
  private static final Logger LOG = LoggerFactory.getLogger(BufferedAsyncDataWriter.class);
  private static final WriteResponseMapper<RecordMetadata> WRITE_RESPONSE_WRAPPER =
      new WriteResponseMapper<RecordMetadata>() {

        @Override
        public WriteResponse wrap(final RecordMetadata recordMetadata) {
          return new WriteResponse<RecordMetadata>() {
            @Override
            public RecordMetadata getRawResponse() {
              return recordMetadata;
            }

            @Override
            public String getStringResponse() {
              return recordMetadata.toString();
            }

            @Override
            public long bytesWritten() {
              // Don't know how many bytes were written
              return -1;
            }
          };
        }
      };

  public BufferedAsyncDataWriter (BatchAccumulator<D> accumulator, BatchAsyncDataWriter<D> dataWriter) {
    this.processor = new RecordProcessor (accumulator, dataWriter);
    this.accumulator = accumulator;
    this.service = Executors.newFixedThreadPool(1);
    this.running = true;
    this.startTime = System.currentTimeMillis();
    try {
      this.service.execute(this.processor);
      this.service.shutdown();
    } catch (Exception e) {
      LOG.error("Cannot start internal thread to consume the data");
    }
  }

  private class RecordProcessor<D> implements Runnable, Closeable{
    BatchAccumulator<D> accumulator;
    BatchAsyncDataWriter<D> writer;

    public void close() throws IOException {
      this.writer.close();
    }

    public RecordProcessor (BatchAccumulator<D> accumulator, BatchAsyncDataWriter<D> writer) {
      this.accumulator = accumulator;
      this.writer = writer;
    }

    public void run() {
      LOG.info ("Start iterating accumulator");

      /**
       * A main loop to process available batches
       */
      while (running) {
        Batch<D> batch = this.accumulator.getNextAvailableBatch();
        if (batch != null) {
          this.writer.write(batch, this.createBatchCallback(batch));
        }
      }

      // Wait until all the ongoing appends finished
      accumulator.waitClose();
      LOG.info ("Start to process remaining batches");

      /**
       * A main loop to process remaining batches
       */
      Batch<D> batch;
      while ((batch = this.accumulator.getNextAvailableBatch()) != null) {
        this.writer.write(batch, this.createBatchCallback(batch));
      }

      // Wait until all the batches get acknowledged
      accumulator.flush();
    }

    /**
     * A callback which handles the post-processing logic after a batch has sent out and
     * receives the result
     */
    private WriteCallback createBatchCallback (final Batch<D> batch) {
      return new WriteCallback<Object>() {
        @Override
        public void onSuccess(WriteResponse writeResponse) {
          LOG.info ("Batch " + batch.getId() + " is on success with size " + batch.getCurrentSizeInByte() + " num of record " + batch.getRecords().size());
          batch.onSuccess(writeResponse);
          batch.done();
          accumulator.deallocate(batch);
        }

        @Override
        public void onFailure(Throwable throwable) {
          LOG.info ("Batch " + batch.getId() + " is on failure");
          batch.onFailure(throwable);
          batch.done();
          accumulator.deallocate(batch);
        }
      };
    }
  }

  /**
   * Asynchronously write a record, execute the callback on success/failure
   */
  public Future<WriteResponse> write(D record, @Nullable WriteCallback callback) {
    try {
      Future<RecordMetadata> future = this.accumulator.append(record, callback);
      return new WriteResponseFuture (future, WRITE_RESPONSE_WRAPPER);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Flushes all pending writes
   */
  public void flush() throws IOException {
    this.accumulator.flush();
  }

  /**
   * Force to close all the resources and drop all the pending requests
   */
  public void forceClose() {
    LOG.info ("Force to close the buffer data writer (not supported)");
  }

  /**
   * Close all the resources, this will be blocked until all the request are sent and gets acknowledged
   */
  public void close() throws  IOException {
    try {
      this.running = false;
      this.accumulator.close();
      if (!this.service.awaitTermination(60, TimeUnit.SECONDS)) {
        forceClose();
      } else {
        LOG.info ("Closed properly: elapsed " + (System.currentTimeMillis() - startTime) + " milliseconds");
      }
    } catch (InterruptedException e) {
      LOG.error ("Interruption happened during close " + e.toString());
    } finally {
      this.processor.close();
    }
  }
}
