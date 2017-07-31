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

package org.apache.gobblin.ack;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;

import lombok.Getter;
import lombok.RequiredArgsConstructor;


/**
 * An {@link Ackable} used to ack other {@link Ackable}s when a set of children ackables are all acknowledged. This is
 * useful when forking a record, as we don't want to ack the original record until all children have been acked.
 *
 * Usage:
 * HierarchicalAckable ackable = new HierarchicalAckable(list_of_parent_ackables);
 * Ackable childAckable = ackable.newChildAckable();
 * ackable.close(); // Must close HierarchicalAckable to indicate no more children will be created.
 * childAckable.ack(); // When acking all children, parents will be acked.
 */
@RequiredArgsConstructor
public class HierarchicalAckable implements Closeable {

  private final List<Ackable> parentAckables;
  private final AtomicInteger remainingCallbacks = new AtomicInteger();
  private final ConcurrentLinkedQueue<Throwable> throwables = new ConcurrentLinkedQueue<>();
  private volatile boolean closed = false;

  /**
   * @return A new child {@link Ackable} that must be acked before parents are acked.
   */
  public Ackable newChildAckable() {
    if (this.closed) {
      throw new IllegalStateException(HierarchicalAckable.class.getSimpleName() + " is already closed.");
    }
    this.remainingCallbacks.incrementAndGet();
    return new ChildAckable();
  }

  /**
   * Indicates no more children will be created.
   */
  @Override
  public synchronized void close() {
    this.closed = true;
    maybeAck();
  }

  private synchronized void maybeAck() {
    if (this.remainingCallbacks.get() == 0 && this.closed) {
      if (!this.throwables.isEmpty()) {
        ChildrenFailedException exc = new ChildrenFailedException(ImmutableList.copyOf(this.throwables));
        for (Ackable ackable : this.parentAckables) {
          ackable.nack(exc);
        }
      } else {
        for (Ackable ackable : this.parentAckables) {
          ackable.ack();
        }
      }
    }
  }

  private class ChildAckable implements Ackable {
    private volatile boolean acked = false;

    @Override
    public synchronized void ack() {
      if (this.acked) {
        return;
      }
      this.acked = true;
      HierarchicalAckable.this.remainingCallbacks.decrementAndGet();
      maybeAck();
    }

    @Override
    public synchronized void nack(Throwable error) {
      if (this.acked) {
        return;
      }
      this.acked = true;
      HierarchicalAckable.this.remainingCallbacks.decrementAndGet();
      HierarchicalAckable.this.throwables.add(error);
      maybeAck();
    }
  }

  /**
   * Indicates that at least one of the children {@link Ackable}s was nacked.
   */
  public static class ChildrenFailedException extends Exception {
    @Getter
    private final ImmutableList<Throwable> failureCauses;

    private ChildrenFailedException(ImmutableList<Throwable> failureCauses) {
      super("Some child ackables failed.");
      this.failureCauses = failureCauses;
    }
  }

}
