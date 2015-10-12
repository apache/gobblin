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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * {@link java.util.concurrent.ThreadPoolExecutor} that scales from a min to a max number of threads as tasks get
 * added.
 */
public class ScalingThreadPoolExecutor extends ThreadPoolExecutor {

  /**
   * Creates a {@link ScalingThreadPoolExecutor}.
   * @param min Core thread pool size.
   * @param max Max number of threads allowed.
   * @param keepAliveTime Keep alive time for unused threads in milliseconds.
   * @return A {@link ScalingThreadPoolExecutor}.
   */
  public static ScalingThreadPoolExecutor newScalingThreadPool(int min, int max, long keepAliveTime) {
    return newScalingThreadPool(min, max, keepAliveTime, Executors.defaultThreadFactory());
  }

  /**
   * Creates a {@link ScalingThreadPoolExecutor}.
   * @param min Core thread pool size.
   * @param max Max number of threads allowed.
   * @param keepAliveTime Keep alive time for unused threads in milliseconds.
   * @param threadFactory thread factory to use.
   * @return A {@link ScalingThreadPoolExecutor}.
   */
  public static ScalingThreadPoolExecutor newScalingThreadPool(int min, int max,
      long keepAliveTime, ThreadFactory threadFactory) {
    ScalingQueue queue = new ScalingQueue();
    ScalingThreadPoolExecutor executor = new ScalingThreadPoolExecutor(min, max, keepAliveTime,
        TimeUnit.MILLISECONDS, queue, threadFactory);
    executor.setRejectedExecutionHandler(new ForceQueuePolicy());
    queue.setThreadPoolExecutor(executor);
    return executor;
  }

  private ScalingThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
      long keepAliveTime, TimeUnit unit, BlockingQueue workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

}
