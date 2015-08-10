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

package gobblin.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * A utility class to use with {@link java.util.concurrent.Executors} in cases such as when creating new thread pools.
 *
 * @author ynli
 */
public class ExecutorsUtils {

  private static final ThreadFactory DEFAULT_THREAD_FACTORY = newThreadFactory(Optional.<Logger>absent());

  public static final long EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT = 60;
  public static final TimeUnit EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS;

  /**
   * Get a default {@link java.util.concurrent.ThreadFactory}.
   *
   * @return the default {@link java.util.concurrent.ThreadFactory}
   */
  public static ThreadFactory defaultThreadFactory() {
    return DEFAULT_THREAD_FACTORY;
  }

  /**
   * Get a new {@link java.util.concurrent.ThreadFactory} that uses a {@link LoggingUncaughtExceptionHandler}
   * to handle uncaught exceptions.
   *
   * @param logger an {@link com.google.common.base.Optional} wrapping the {@link org.slf4j.Logger} that the
   *               {@link LoggingUncaughtExceptionHandler} uses to log uncaught exceptions thrown in threads
   * @return a new {@link java.util.concurrent.ThreadFactory}
   */
  public static ThreadFactory newThreadFactory(Optional<Logger> logger) {
    return newThreadFactory(logger, Optional.<String>absent());
  }

  /**
   * Get a new {@link java.util.concurrent.ThreadFactory} that uses a {@link LoggingUncaughtExceptionHandler}
   * to handle uncaught exceptions and the given thread name format.
   *
   * @param logger an {@link com.google.common.base.Optional} wrapping the {@link org.slf4j.Logger} that the
   *               {@link LoggingUncaughtExceptionHandler} uses to log uncaught exceptions thrown in threads
   * @param nameFormat an {@link com.google.common.base.Optional} wrapping a thread naming format
   * @return a new {@link java.util.concurrent.ThreadFactory}
   */
  public static ThreadFactory newThreadFactory(Optional<Logger> logger, Optional<String> nameFormat) {
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
    if (nameFormat.isPresent()) {
      builder.setNameFormat(nameFormat.get());
    }
    return builder.setUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler(logger)).build();
  }

  /**
   * Shutdown an {@link ExecutorService} gradually, first disabling new task submissions and later cancelling
   * existing tasks.
   *
   * <p>
   *   The implementation is based on the implementation of Guava's MoreExecutors.shutdownAndAwaitTermination,
   *   which is available since version 17.0. We cannot use Guava version 17.0 or after directly, however, as
   *   it cannot be used with Hadoop 2.6.0 or after due to the issue reported in HADOOP-10961.
   * </p>
   *
   * @param executorService the {@link ExecutorService} to shutdown
   * @param timeout the maximum time to wait for the {@code ExecutorService} to terminate
   * @param unit the time unit of the timeout argument
   */
  public static void shutdownExecutorService(ExecutorService executorService, long timeout, TimeUnit unit) {
    Preconditions.checkNotNull(unit);
    // Disable new tasks from being submitted
    executorService.shutdown();
    try {
      long halfTimeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit) / 2;
      // Wait for half the duration of the timeout for existing tasks to terminate
      if (!executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
        // Cancel currently executing tasks
        executorService.shutdownNow();
        // Wait the other half of the timeout for tasks to respond to being cancelled
        executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
      }
    } catch (InterruptedException ie) {
      // Preserve interrupt status
      Thread.currentThread().interrupt();
      // (Re-)Cancel if current thread also interrupted
      executorService.shutdownNow();
    }
  }

  /**
   * Shutdown an {@link ExecutorService} gradually, first disabling new task submissions and
   * later cancelling existing tasks.
   *
   * <p>
   *   This method calls {@link #shutdownExecutorService(ExecutorService, long, TimeUnit)}
   *   with default timeout time {@link #EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT} and time unit
   *   {@link #EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT}.
   * </p>
   *
   * @param executorService the {@link ExecutorService} to shutdown
   */
  public static void shutdownExecutorService(ExecutorService executorService) {
    shutdownExecutorService(executorService, EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT,
        EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT);
  }
}
