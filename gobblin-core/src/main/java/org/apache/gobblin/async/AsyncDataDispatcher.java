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

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractExecutionThreadService;

import javax.annotation.concurrent.NotThreadSafe;


/**
 * Base class with skeleton logic to dispatch a record asynchronously. It buffers the records and consumes
 * them by {@link #run()}
 *
 * <p>
 *   However the records are consumed depends on the actual implementation of {@link #dispatch(Queue)}, which
 *   may process one record or a batch at a time
 * </p>
 *
 * @param <D> type of record
 */
@NotThreadSafe
public abstract class AsyncDataDispatcher<D> extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncDataDispatcher.class);

  // Queue to buffer records
  private final BlockingQueue<D> buffer;

  // Lock for isBufferEmpty condition
  private final Lock lock;
  private final Condition isBufferEmpty;

  public AsyncDataDispatcher(int capacity) {
    super();
    buffer = new ArrayBlockingQueue<>(capacity);
    lock = new ReentrantLock(true);
    isBufferEmpty = lock.newCondition();
    startAsync();
    awaitRunning();
  }

  /**
   * Synchronously dispatch records in the buffer. Retries should be done if necessary. Every record
   * consumed from the buffer must have its callback called if any.
   *
   * @param buffer the buffer which contains a collection of records
   * @throws DispatchException if dispatch failed
   */
  protected abstract void dispatch(Queue<D> buffer)
      throws DispatchException;

  protected void put(D record) {
    // Accept new record only if dispatcher is running
    checkRunning("put");
    try {
      buffer.put(record);
      // Check after a blocking put
      if (!isRunning()) {
        // Purge out the record which was just put into the buffer
        buffer.clear();
        RuntimeException e = new RuntimeException("Attempt to operate when writer is " + state().name());
        LOG.error("put", e);
        throw e;
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Waiting to put a record interrupted", e);
    }
  }

  @Override
  protected void run()
      throws Exception {
    LOG.info("Start processing records");
    // A main loop to process records
    while (true) {
      while (buffer.isEmpty()) {
        // Buffer is empty
        notifyBufferEmptyOccurrence();
        if (!isRunning()) {
          // Clean return
          return;
        }
        // Waiting for some time to get some records
        try {
          Thread.sleep(300);
        } catch (InterruptedException e) {
          LOG.warn("Dispatcher sleep interrupted", e);
          break;
        }
      }

      // Dispatch records
      try {
        dispatch(buffer);
      } catch (DispatchException e) {
        LOG.error("Dispatch incurs an exception", e);
        if (e.isFatal()) {
          // Mark stopping
          stopAsync();
          // Drain the buffer
          buffer.clear();
          // Wake up the threads waiting on buffer empty occurrence
          notifyBufferEmptyOccurrence();
          throw e;
        }
      }
    }
  }

  /**
   * A blocking terminate
   */
  public void terminate() {
    stopAsync().awaitTerminated();
  }

  protected void checkRunning(String forWhat) {
    if (!isRunning()) {
      RuntimeException e = new RuntimeException("Attempt to operate when writer is " + state().name());
      LOG.error(forWhat, e);
      throw e;
    }
  }

  protected void waitForBufferEmpty() {
    checkRunning("waitForBufferEmpty");
    try {
      lock.lock();
      // Waiting buffer empty
      while (!buffer.isEmpty()) {
        try {
          isBufferEmpty.await();
        } catch (InterruptedException e) {
          throw new RuntimeException("Waiting for buffer flush interrupted", e);
        }
      }
    } finally {
      lock.unlock();
      checkRunning("waitForBufferEmpty");
    }
  }

  private void notifyBufferEmptyOccurrence() {
    try {
      lock.lock();
      isBufferEmpty.signalAll();
    } finally {
      lock.unlock();
    }
  }
}
