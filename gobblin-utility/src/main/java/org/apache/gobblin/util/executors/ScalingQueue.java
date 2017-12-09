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

package org.apache.gobblin.util.executors;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Implementation of {@link LinkedBlockingQueue} that is aware of a {@link ThreadPoolExecutor} and rejects insertions
 * if there are no threads available. Used for {@link ScalingThreadPoolExecutor}.
 *
 * <p>
 *   Although this class is {@link java.io.Serializable} because it inherits from {@link LinkedBlockingQueue}, it
 *   is not intended to be serialized (e.g. executor is transient, so will not deserialize correctly).
 * </p>
 */
class ScalingQueue extends LinkedBlockingQueue<Runnable> {

  private static final long serialVersionUID = -4522307109241425248L;

  /**
   * The executor this Queue belongs to.
   */
  private transient ThreadPoolExecutor executor;

  /**
   * Creates a TaskQueue with a capacity of {@link Integer#MAX_VALUE}.
   */
  public ScalingQueue() {
    super();
  }

  /**
   * Creates a TaskQueue with the given (fixed) capacity.
   * @param capacity the capacity of this queue.
   */
  public ScalingQueue(int capacity) {
    super(capacity);
  }

  /**
   * Sets the executor this queue belongs to.
   */
  public synchronized void setThreadPoolExecutor(ThreadPoolExecutor executor) {
    this.executor = executor;
  }

  /**
   * Inserts the specified element at the tail of this queue if there is at least one available thread to run the
   * current task. If all pool threads are actively busy, it rejects the offer.
   *
   * @param runnable the element to add.
   * @return true if it was possible to add the element to this queue, else false
   * @see ThreadPoolExecutor#execute(Runnable)
   */
  @Override
  public synchronized boolean offer(Runnable runnable) {
    int allWorkingThreads = this.executor.getActiveCount() + super.size();
    return allWorkingThreads < this.executor.getPoolSize() && super.offer(runnable);
  }
}
