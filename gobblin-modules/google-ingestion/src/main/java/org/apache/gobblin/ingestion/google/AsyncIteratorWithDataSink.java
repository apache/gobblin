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
package org.apache.gobblin.ingestion.google;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class AsyncIteratorWithDataSink<T> implements Iterator<T> {
  private final Object lock = new Object();
  private volatile Throwable exceptionInProducerThread = null;
  private Thread _producerThread;
  protected LinkedBlockingDeque<T> _dataSink;
  private final int _pollBlockingTime;
  private T _next = null;

  protected AsyncIteratorWithDataSink(int queueSize, int pollBlockingTime) {
    log.info(String.format("Setting queue size: %d, poll blocking second: %d", queueSize, pollBlockingTime));
    _dataSink = new LinkedBlockingDeque<>(queueSize);
    _pollBlockingTime = pollBlockingTime;
  }

  @Override
  public boolean hasNext() {
    initialize();
    if (_next != null) {
      return true;
    }
    //if _next doesn't exist, try polling the next one.
    try {
      _next = _dataSink.poll(_pollBlockingTime, TimeUnit.SECONDS);
      while (_next == null) {
        if (_producerThread.isAlive()) {
          log.info(String.format("Producer job not done yet. Will re-poll for %s second(s)...", _pollBlockingTime));
          _next = _dataSink.poll(_pollBlockingTime, TimeUnit.SECONDS);
          continue;
        }

        synchronized (lock) {
          if (exceptionInProducerThread != null) {
            throw new RuntimeException(
                String.format("Found exception in producer thread %s", _producerThread.getName()),
                exceptionInProducerThread);
          }
        }
        log.info("Producer job done. No more data in the queue.");
        return false;
      }
      return true;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void initialize() {
    if (_producerThread == null) {
      _producerThread = new Thread(getProducerRunnable());
      _producerThread.setUncaughtExceptionHandler(getExceptionHandler());
      _producerThread.start();
    }
  }

  protected abstract Runnable getProducerRunnable();

  @Override
  public T next() {
    if (hasNext()) {
      T toReturn = _next;
      _next = null;
      return toReturn;
    }
    throw new NoSuchElementException();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private Thread.UncaughtExceptionHandler getExceptionHandler() {
    return new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        synchronized (lock) {
          exceptionInProducerThread = e;
        }
      }
    };
  }
}
