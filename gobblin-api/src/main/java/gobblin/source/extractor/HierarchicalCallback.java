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

package gobblin.source.extractor;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;

import gobblin.writer.Ackable;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * A class used to generate forked callbacks. The class takes an {@link Ackable} as an input, which will be acked only when
 * all of the child callbacks are called.
 */
@RequiredArgsConstructor
public class HierarchicalCallback implements Closeable {

  private final Ackable ackable;
  private final AtomicInteger remainingCallbacks = new AtomicInteger();
  private final Queue<Throwable> throwables = new LinkedList<>();
  private volatile boolean closed = false;

  /**
   * @return a new child callback for this {@link HierarchicalCallback}.
   */
  public Callback newChildCallback() {
    if (this.closed) {
      throw new IllegalStateException(HierarchicalCallback.class.getSimpleName() + " is already closed.");
    }
    this.remainingCallbacks.incrementAndGet();
    return new ChildCallback();
  }

  /**
   * Indicates that no new children callbacks will be created.
   */
  @Override
  public synchronized void close() {
    this.closed = true;
    if (this.remainingCallbacks.get() == 0) {
      this.ackable.ack();
    }
  }

  private synchronized void maybeAck() {
    if (this.remainingCallbacks.get() == 0 && this.closed) {
      if (!this.throwables.isEmpty()) {
        this.ackable.nack(new ChildrenFailedException(ImmutableList.copyOf(this.throwables)));
      } else {
        this.ackable.ack();
      }
    }
  }

  public static class ChildrenFailedException extends Exception {
    @Getter
    private final ImmutableList<Throwable> failureCauses;

    public ChildrenFailedException(ImmutableList<Throwable> failureCauses) {
      super("Some child callbacks failed.");
      this.failureCauses = failureCauses;
    }
  }

  private class ChildCallback implements Callback {
    private volatile boolean acked = false;

    @Override
    public synchronized void onSuccess() {
      if (this.acked) {
        return;
      }
      this.acked = true;
      HierarchicalCallback.this.remainingCallbacks.decrementAndGet();
      maybeAck();
    }

    @Override
    public synchronized void onFailure(Throwable cause) {
      if (this.acked) {
        return;
      }
      this.acked = true;
      HierarchicalCallback.this.remainingCallbacks.decrementAndGet();
      HierarchicalCallback.this.throwables.add(cause);
      maybeAck();
    }
  }

}
