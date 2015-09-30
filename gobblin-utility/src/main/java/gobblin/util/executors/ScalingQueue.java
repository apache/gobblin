/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.executors;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Implementation of {@link LinkedBlockingQueue} that is aware of a {@link ThreadPoolExecutor} and rejects insertions
 * if there are no threads available.
 */
public class ScalingQueue extends LinkedBlockingQueue<Runnable> {
  /**
   * The executor this Queue belongs to
   */
  private ThreadPoolExecutor executor;

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
  public void setThreadPoolExecutor(ThreadPoolExecutor executor) {
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
    int allWorkingThreads = executor.getActiveCount() + super.size();
    return allWorkingThreads < executor.getPoolSize() && super.offer(runnable);
  }
}
