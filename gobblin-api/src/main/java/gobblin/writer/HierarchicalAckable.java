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

package gobblin.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;

import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class HierarchicalAckable implements Closeable {

  private final List<Ackable> parentAckables;
  private final AtomicInteger remainingCallbacks = new AtomicInteger();
  private final ConcurrentLinkedQueue<Throwable> throwables = new ConcurrentLinkedQueue<>();
  private volatile boolean closed = false;

  public Ackable newChildAckable() {
    if (this.closed) {
      throw new IllegalStateException(HierarchicalAckable.class.getSimpleName() + " is already closed.");
    }
    this.remainingCallbacks.incrementAndGet();
    return new ChildAckable();
  }

  @Override
  public synchronized void close() throws IOException {
    this.closed = true;
    maybeAck();
  }

  public synchronized void maybeAck() {
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

  public static class ChildrenFailedException extends Exception {
    private final ImmutableList<Throwable> failureCauses;

    public ChildrenFailedException(ImmutableList<Throwable> failureCauses) {
      super("Some child ackables failed.");
      this.failureCauses = failureCauses;
    }
  }

}
