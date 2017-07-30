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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;

import gobblin.annotation.Alpha;

/**
 * A future object generated after a record was inserted into a batch
 * We can include more meta data about this record in the future
 */
@Alpha
public final class RecordFuture implements Future<RecordMetadata>{
  CountDownLatch latch;
  long offset;

  public RecordFuture (CountDownLatch latch, long offset) {
    this.latch = latch;
    this.offset = offset;
  }

  @Override
  public boolean isDone() {
    return this.latch.getCount() == 0L;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean cancel(boolean interrupt) {
    return false;
  }

  @Override
  public RecordMetadata get() throws InterruptedException, ExecutionException {
    this.latch.await();
    return new RecordMetadata(this.offset);
  }

  @Override
  public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    boolean occurred = this.latch.await(timeout, unit);
    if (!occurred) {
      throw new TimeoutException("Timeout after waiting for " + TimeUnit.MILLISECONDS.convert(timeout, unit));
    }
    return new RecordMetadata(this.offset);
  }
}