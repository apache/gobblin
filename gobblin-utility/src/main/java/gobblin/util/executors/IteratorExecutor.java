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

package gobblin.util.executors;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;


/**
 * Executes tasks in an {@link Iterator}. Tasks need not be generated until they can be executed.
 * @param <T>
 */
@Slf4j
public class IteratorExecutor<T> {

  private final CompletionService<T> completionService;
  private final int numThreads;
  private final ExecutorService executor;
  private final Iterator<Callable<T>> iterator;
  private boolean executed;

  public IteratorExecutor(Iterator<Callable<T>> runnableIterator, int numThreads, ThreadFactory threadFactory) {
    this.numThreads = numThreads;
    this.iterator = runnableIterator;
    this.executor = Executors.newFixedThreadPool(numThreads, threadFactory);
    this.completionService = new ExecutorCompletionService<>(this.executor);
    this.executed = false;
  }

  /**
   * Execute the tasks in the task {@link Iterator}. Blocks until all tasks are completed.
   *
   * <p>
   *  Note: this method only guarantees tasks have finished, not that they have finished successfully. It is the caller's
   *  responsibility to verify the returned futures are successful. Also see {@link #executeAndGetResults()} for different
   *  semantics.
   * </p>
   *
   * @return a list of completed futures.
   * @throws InterruptedException
   */
  public List<Future<T>> execute() throws InterruptedException {

    List<Future<T>> futures = Lists.newArrayList();

    try {
      if (this.executed) {
        throw new RuntimeException(String.format("This %s has already been executed.", IteratorExecutor.class.getSimpleName()));
      }

      int activeTasks = 0;
      while (this.iterator.hasNext()) {
        try {
          futures.add(this.completionService.submit(this.iterator.next()));
          activeTasks++;
        } catch (Exception exception) {
          // if this.iterator.next fails, add an immediate fail future
          futures.add(Futures.<T>immediateFailedFuture(exception));
        }
        if (activeTasks == this.numThreads) {
          this.completionService.take();
          activeTasks--;
        }
      }
      while (activeTasks > 0) {
        this.completionService.take();
        activeTasks--;
      }
    } finally {
      ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(log), 10, TimeUnit.SECONDS);
      this.executed = true;
    }

    return futures;
  }

  /**
   * Execute the tasks in the task {@link Iterator}. Blocks until all tasks are completed. Gets the results of each
   * task, and for each task returns either its result or the thrown {@link ExecutionException}.
   *
   * @return a list containing for each task either its result or the {@link ExecutionException} thrown, in the same
   *        order as the input {@link Iterator}.
   * @throws InterruptedException
   */
  public List<Either<T, ExecutionException>> executeAndGetResults() throws InterruptedException {
    List<Either<T, ExecutionException>> results = Lists.newArrayList();
    List<Future<T>> futures = execute();
    for (Future<T> future : futures) {
      try {
        results.add(Either.<T, ExecutionException>left(future.get()));
      } catch (ExecutionException ee) {
        results.add(Either.<T, ExecutionException>right(ee));
      }
    }
    return results;
  }

  /**
   * Utility method that checks whether all tasks succeeded from the output of {@link #executeAndGetResults()}.
   * @return true if all tasks succeeded.
   */
  public static <T> boolean verifyAllSuccessful(List<Either<T, ExecutionException>> results) {
    return Iterables.all(results, new Predicate<Either<T, ExecutionException>>() {
      @Override
      public boolean apply(@Nullable Either<T, ExecutionException> input) {
        return input instanceof Either.Left;
      }
    });
  }

  /**
   * Log failures in the output of {@link #executeAndGetResults()}.
   * @param results output of {@link #executeAndGetResults()}
   * @param useLogger logger to log the messages into.
   * @param atMost will log at most this many errors.
   */
  public static <T> void logFailures(List<Either<T, ExecutionException>> results, Logger useLogger, int atMost) {
    Logger actualLogger = useLogger == null ? log : useLogger;
    Iterator<Either<T, ExecutionException>> it = results.iterator();
    int printed = 0;
    while (it.hasNext()) {
      Either<T, ExecutionException> nextResult = it.next();
      if (nextResult instanceof Either.Right) {
        ExecutionException exc = ((Either.Right<T, ExecutionException>) nextResult).getRight();
        actualLogger.error("Iterator executor failure.", exc);
        printed++;
        if (printed >= atMost) {
          return;
        }
      }
    }
  }

}
