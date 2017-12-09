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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import lombok.Builder;


/**
 * Utility class for exponential backoff.
 *
 * Usage:
 * ExponentialBackoff exponentialBackoff = ExponentialBackoff.builder().build();
 * exponentialBackoff.awaitNextRetry();
 */
public class ExponentialBackoff {

  private final double alpha;
  private final long maxWait;
  private final int maxRetries;
  private final long maxDelay;

  private int retryNumber = 0;
  private long totalWait = 0;
  private long nextDelay;

  /**
   * @param alpha the multiplier for each backoff iteration. (def: 2)
   * @param maxRetries maximum number of retries allowed. (def: infinite)
   * @param maxWait maximum wait allowed in millis. (def: infinite)
   * @param maxDelay maximum delay allowed in millis. (def: infinite)
   * @param initialDelay initial delay in millis. (def: 20)
   */
  @Builder
  private ExponentialBackoff(Double alpha, Integer maxRetries, Long maxWait, Long maxDelay, Long initialDelay) {
    this.alpha = alpha == null ? 2 : alpha;
    this.maxRetries = maxRetries == null ? Integer.MAX_VALUE : maxRetries;
    this.maxWait = maxWait == null ? Long.MAX_VALUE : maxWait;
    this.maxDelay = maxDelay == null ? Long.MAX_VALUE : maxDelay;
    this.nextDelay = initialDelay == null ? 20 : initialDelay;
  }

  /**
   * Block until next retry can be executed.
   *
   * This method throws an exception if the max number of retries has been reached. For an alternative see
   * {@link #awaitNextRetryIfAvailable()}.
   *
   * @throws NoMoreRetriesException If maximum number of retries has been reached.
   */
  public void awaitNextRetry() throws InterruptedException, NoMoreRetriesException {
    this.retryNumber++;
    if (this.retryNumber > this.maxRetries) {
      throw new NoMoreRetriesException("Reached maximum number of retries: " + this.maxRetries);
    } else if (this.totalWait > this.maxWait) {
      throw new NoMoreRetriesException("Reached maximum time to wait: " + this.maxWait);
    }
    Thread.sleep(this.nextDelay);
    this.totalWait += this.nextDelay;
    this.nextDelay = Math.min((long) (this.alpha * this.nextDelay) + 1, this.maxDelay);
  }

  /**
   * Block until next retry can be executed unless max retries has been reached.
   *
   * This method uses the return value to specify if a retry is allowed, which the caller should respect. For an alternative see
   * {@link #awaitNextRetry()}.
   *
   * @return true if the next execution can be run, false if the max number of retries has been reached.
   */
  public boolean awaitNextRetryIfAvailable() throws InterruptedException {
    try {
      awaitNextRetry();
      return true;
    } catch (NoMoreRetriesException exc) {
      return false;
    }
  }

  /**
   * Thrown if no more retries are available for {@link ExponentialBackoff}.
   */
  public static class NoMoreRetriesException extends Exception {
    public NoMoreRetriesException(String message) {
      super(message);
    }
  }

  /**
   * Evaluate a condition until true with exponential backoff.
   * @param callable Condition.
   * @return true if the condition returned true.
   * @throws ExecutionException if the condition throws an exception.
   */
  @Builder(builderMethodName = "awaitCondition",  buildMethodName = "await")
  private static boolean evaluateConditionUntilTrue(Callable<Boolean> callable, Double alpha, Integer maxRetries,
      Long maxWait, Long maxDelay, Long initialDelay) throws ExecutionException, InterruptedException {
    ExponentialBackoff exponentialBackoff = new ExponentialBackoff(alpha, maxRetries, maxWait, maxDelay, initialDelay);
    while (true) {
      try {
        if (callable.call()) {
          return true;
        }
      } catch (Throwable t) {
        throw new ExecutionException(t);
      }
      if (!exponentialBackoff.awaitNextRetryIfAvailable()) {
        return false;
      }
    }
  }

}
