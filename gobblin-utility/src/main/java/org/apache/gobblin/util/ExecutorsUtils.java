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

package org.apache.gobblin.util;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.apache.gobblin.util.executors.MDCPropagatingCallable;
import org.apache.gobblin.util.executors.MDCPropagatingRunnable;
import org.apache.gobblin.util.executors.MDCPropagatingScheduledExecutorService;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.gobblin.util.executors.MDCPropagatingExecutorService;


/**
 * A utility class to use with {@link java.util.concurrent.Executors} in cases such as when creating new thread pools.
 *
 * @author Yinan Li
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
    return newThreadFactory(new ThreadFactoryBuilder(), logger, nameFormat);
  }

  /**
   * Get a new {@link ThreadFactory} that uses a {@link LoggingUncaughtExceptionHandler}
   * to handle uncaught exceptions, uses the given thread name format, and produces daemon threads.
   *
   * @param logger an {@link Optional} wrapping the {@link Logger} that the
   *               {@link LoggingUncaughtExceptionHandler} uses to log uncaught exceptions thrown in threads
   * @param nameFormat an {@link Optional} wrapping a thread naming format
   * @return a new {@link ThreadFactory}
   */
  public static ThreadFactory newDaemonThreadFactory(Optional<Logger> logger, Optional<String> nameFormat) {
    return newThreadFactory(new ThreadFactoryBuilder().setDaemon(true), logger, nameFormat);
  }

  /**
   * Get a new {@link ThreadFactory} that uses a {@link LoggingUncaughtExceptionHandler}
   * to handle uncaught exceptions.
   * Tasks running within such threads will have the same access control and class loader settings as the
   * thread that invokes this method.
   *
   * @param logger an {@link Optional} wrapping the {@link Logger} that the
   *               {@link LoggingUncaughtExceptionHandler} uses to log uncaught exceptions thrown in threads
   * @return a new {@link ThreadFactory}
   */
  public static ThreadFactory newPrivilegedThreadFactory(Optional<Logger> logger) {
    return newThreadFactory(new ThreadFactoryBuilder().setThreadFactory(Executors.privilegedThreadFactory()), logger,
        Optional.<String>absent());
  }

  private static ThreadFactory newThreadFactory(ThreadFactoryBuilder builder, Optional<Logger> logger,
      Optional<String> nameFormat) {
    if (nameFormat.isPresent()) {
      builder.setNameFormat(nameFormat.get());
    }
    return builder.setUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler(logger)).build();
  }

  /**
   * Creates an {@link ListeningExecutorService} whose {@code submit}
   * and {@code execute} methods propagate the MDC information across
   * thread boundaries.
   * @param executorService the {@link ExecutorService} to wrap
   * @return a new instance of {@link ListeningExecutorService}
   */
  public static ListeningExecutorService loggingDecorator(ExecutorService executorService) {
    return new MDCPropagatingExecutorService(executorService);
  }

  /**
   * Creates an {@link ListeningScheduledExecutorService} whose
   * {@code submit}, {@code execute}, {@code schedule},
   * {@code scheduleAtFixedRate}, and {@code scheduleWithFixedDelay}
   * methods propagate the MDC information across thread boundaries.
   * @param scheduledExecutorService the {@link ScheduledExecutorService} to wrap
   * @return a new instance of {@link ListeningScheduledExecutorService}
   */
  public static ListeningScheduledExecutorService loggingDecorator(ScheduledExecutorService scheduledExecutorService) {
    return new MDCPropagatingScheduledExecutorService(scheduledExecutorService);
  }

  /**
   * Creates an {@link Runnable} which propagates the MDC
   * information across thread boundaries.
   * @param runnable the {@link Runnable} to wrap
   * @return a new instance of {@link Runnable}
   */
  public static Runnable loggingDecorator(Runnable runnable) {
    if (runnable instanceof MDCPropagatingRunnable) {
      return runnable;
    }
    return new MDCPropagatingRunnable(runnable);
  }

  /**
   * Creates an {@link Callable<T>} which propagates the MDC
   * information across thread boundaries.
   * @param callable the {@link Callable<T>} to wrap
   * @return a new instance of {@link Callable<T>}
   */
  public static <T> Callable<T> loggingDecorator(Callable<T> callable) {
    if (callable instanceof MDCPropagatingCallable) {
      return callable;
    }
    return new MDCPropagatingCallable<T>(callable);
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
   * @param logger an {@link Optional} wrapping the {@link Logger} that is used to log metadata of the executorService
   *               if it cannot shutdown all its threads
   * @param timeout the maximum time to wait for the {@code ExecutorService} to terminate
   * @param unit the time unit of the timeout argument
   */
  public static void shutdownExecutorService(ExecutorService executorService, Optional<Logger> logger, long timeout,
      TimeUnit unit) {
    Preconditions.checkNotNull(unit);
    // Disable new tasks from being submitted
    executorService.shutdown();

    if (logger.isPresent()) {
      logger.get().info("Attempting to shutdown ExecutorService: " + executorService);
    }

    try {
      long halfTimeoutNanos = TimeUnit.NANOSECONDS.convert(timeout, unit) / 2;
      // Wait for half the duration of the timeout for existing tasks to terminate
      if (!executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
        // Cancel currently executing tasks
        executorService.shutdownNow();

        if (logger.isPresent()) {
          logger.get().info("Shutdown un-successful, attempting shutdownNow of ExecutorService: " + executorService);
        }

        // Wait the other half of the timeout for tasks to respond to being cancelled
        if (!executorService.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS) && logger.isPresent()) {
          logger.get().error("Could not shutdown all threads in ExecutorService: " + executorService);
        }
      } else if (logger.isPresent()) {
        logger.get().info("Successfully shutdown ExecutorService: " + executorService);
      }
    } catch (InterruptedException ie) {
      // Preserve interrupt status
      Thread.currentThread().interrupt();
      // (Re-)Cancel if current thread also interrupted
      executorService.shutdownNow();

      if (logger.isPresent()) {
        logger.get().info("Attempting to shutdownNow ExecutorService: " + executorService);
      }
    }
  }

  /**
   * Shutdown an {@link ExecutorService} gradually, first disabling new task submissions and
   * later cancelling existing tasks.
   *
   * <p>
   *   This method calls {@link #shutdownExecutorService(ExecutorService, Optional, long, TimeUnit)}
   *   with default timeout time {@link #EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT} and time unit
   *   {@link #EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT}.
   * </p>
   *
   * @param executorService the {@link ExecutorService} to shutdown
   * @param logger an {@link Optional} wrapping a {@link Logger} to be used during shutdown
   */
  public static void shutdownExecutorService(ExecutorService executorService, Optional<Logger> logger) {
    shutdownExecutorService(executorService, logger, EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT,
        EXECUTOR_SERVICE_SHUTDOWN_TIMEOUT_TIMEUNIT);
  }

  /**
   * A utility method to parallelize loops. Applies the {@link Function} to every element in the {@link List} in
   * parallel by spawning threads. A list containing the result obtained by applying the function is returned. The
   * method is a blocking call and will wait for all the elements in the list to be processed or timeoutInSecs which
   * ever is earlier.
   * <p>
   * <b>NOTE: The method is an all or none implementation. Meaning, if any of the thread fails, the method will throw an
   * {@link ExecutionException} even if other threads completed successfully</b>
   * </p>
   *
   * <ul>
   * <li>Uses a Fixed thread pool of size threadCount.
   * <li>Uses {@link #shutdownExecutorService(ExecutorService, Optional, long, TimeUnit)} to shutdown the executor
   * service
   * <li>All threads are daemon threads
   * </ul>
   *
   * @param list input list on which the function is applied in parallel
   * @param function to be applied on every element of the list
   * @param threadCount to be used to process the list
   * @param timeoutInSecs to wait for all the threads to complete
   * @param logger an {@link Optional} wrapping a {@link Logger} to be used during shutdown
   *
   * @return a list containing the result obtained by applying the function on each element of the input list in the
   *         same order
   *
   * @throws IllegalArgumentException if input list or function is null
   * @throws ExecutionException <ul>
   *           <li>if any computation threw an exception
   *           <li>if any computation was cancelled
   *           <li>if any thread was interrupted while waiting
   *           <ul>
   */
  public static <F, T> List<T> parallelize(final List<F> list, final Function<F, T> function, int threadCount,
      int timeoutInSecs, Optional<Logger> logger) throws ExecutionException {

    Preconditions.checkArgument(list != null, "Input list can not be null");
    Preconditions.checkArgument(function != null, "Function can not be null");

    final List<T> results = Lists.newArrayListWithCapacity(list.size());
    List<Future<T>> futures = Lists.newArrayListWithCapacity(list.size());

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount, ExecutorsUtils.newThreadFactory(logger)), 2,
            TimeUnit.MINUTES);

    for (final F l : list) {
      futures.add(executorService.submit(new Callable<T>() {
        @Override
        public T call() throws Exception {
          return function.apply(l);
        }
      }));
    }

    ExecutorsUtils.shutdownExecutorService(executorService, logger, timeoutInSecs, TimeUnit.SECONDS);

    for (Future<T> future : futures) {
      try {
        results.add(future.get());
      } catch (InterruptedException e) {
        throw new ExecutionException("Thread interrupted", e);
      }
    }

    return results;
  }
}
