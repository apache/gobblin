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
package org.apache.gobblin.testing;

import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;


/**
 * A helper class to perform a check for a condition with a back-off. Primary use of this is when a tests
   * needs for something asynchronous to happen, say a service to start.
 **/
public class AssertWithBackoff {

  /** the max time in milliseconds to wait for the condition to become true */
  private long timeoutMs = 30 * 1000;
  /** a logger to use for logging waiting, results, etc. */
  private Logger log = LoggerFactory.getLogger(AssertWithBackoff.class);
  /** the number to multiple the sleep after condition failure */
  private Optional<Double> backoffFactor = Optional.<Double> absent();
  /** the max time to sleep between condition failures; */
  private Optional<Long> maxSleepMs = Optional.<Long> absent();

  public class EqualsCheck<T> implements Predicate<Void> {
    private final Predicate<T> eqToExpected;
    private final String message;
    private final Function<Void, T> actual;

    public EqualsCheck(Function<Void, T> actual, T expected, String message) {
      this.eqToExpected = Predicates.equalTo(expected);
      this.message = message;
      this.actual = actual;
    }

    @Override
    public boolean apply(Void input) {
      T currentValue = this.actual.apply(input);
      getLogger().debug("checking '" + this.message + "': " + currentValue);
      return this.eqToExpected.apply(currentValue);
    }

  }

  /** Creates a new instance */
  public static AssertWithBackoff create() {
    return new AssertWithBackoff();
  }

  /** Set the max time in milliseconds to wait for the condition to become true */
  public AssertWithBackoff timeoutMs(long assertTimeoutMs) {
    this.timeoutMs = assertTimeoutMs;
    if (!this.maxSleepMs.isPresent()) {
      this.maxSleepMs = Optional.of(getAutoMaxSleep());
    }
    if (!this.backoffFactor.isPresent()) {
      this.backoffFactor = Optional.of(getAutoBackoffFactor());
    }
    return this;
  }

  /** the max time in milliseconds to wait for the condition to become true */
  public long getTimeoutMs() {
    return this.timeoutMs;
  }

  /** Set the max time to sleep between condition failures */
  public AssertWithBackoff maxSleepMs(long maxSleepMs) {
    this.maxSleepMs = Optional.of(maxSleepMs);
    return this;
  }

  /** The max time to sleep between condition failures */
  public long getMaxSleepMs() {
    return this.maxSleepMs.or(getAutoMaxSleep());
  }

  /** Set the number to multiple the sleep after condition failure */
  public AssertWithBackoff backoffFactor(double backoffFactor) {
    this.backoffFactor = Optional.of(backoffFactor);
    return this;
  }

  /** The number to multiple the sleep after condition failure */
  public double getBackoffFactor() {
    return this.backoffFactor.or(getAutoBackoffFactor());
  }

  /** Set the logger to use for logging waiting, results, etc. */
  public AssertWithBackoff logger(Logger log) {
    this.log = log;
    return this;
  }

  /** The logger to use for logging waiting, results, etc. */
  public Logger getLogger() {
    return this.log;
  }

  private long getAutoMaxSleep() {
    return this.timeoutMs / 3;
  }

  private double getAutoBackoffFactor() {
    return Math.log(getMaxSleepMs()) / Math.log(5);
  }

  /**
   * Performs a check for a condition with a back-off. Primary use of this is when a tests
   * needs for something asynchronous to happen, say a service to start.
   *
   * @param condition           the condition to wait for
   * @param assertTimeoutMs     the max time in milliseconds to wait for the condition to become true
   * @param message             a message to print while waiting for the condition
   * @param log                 a logger to use for logging waiting, results
   * @throws TimeoutException   if the condition did not become true in the specified time budget
   */
  public void assertTrue(Predicate<Void> condition, String message) throws TimeoutException, InterruptedException {
    AssertWithBackoff.assertTrue(condition, getTimeoutMs(), message, getLogger(), getBackoffFactor(), getMaxSleepMs());
  }

  /**
   * A convenience method for {@link #assertTrue(Predicate, String)} to keep checking until a
   * certain value until it becomes equal to an expected value.
   * @param actual    a function that checks the actual value
   * @param expected  the expected value
   * @param message   a debugging message
   **/
  public <T> void assertEquals(Function<Void, T> actual, T expected, String message)
      throws TimeoutException, InterruptedException {
    assertTrue(new EqualsCheck<>(actual, expected, message), message);
  }

  /**
   * Performs a check for a condition with a back-off. Primary use of this is when a tests
   * needs for something asynchronous to happen, say a service to start.
   *
   * @param condition           the condition to wait for
   * @param assertTimeoutMs     the max time in milliseconds to wait for the condition to become true
   * @param message             the message to print while waiting for the condition
   * @param log                 the logger to use for logging waiting, results
   * @param backoffFactor       the number to multiple the sleep after condition failure;
   * @param maxSleepMs          the max time to sleep between condition failures; default is
   * @throws TimeoutException   if the condition did not become true in the specified time budget
   * @throws InterrupedException if the assert gets interrupted while waiting for the condition to
   *                            become true.
   */
  public static void assertTrue(Predicate<Void> condition, long assertTimeoutMs, String message, Logger log,
      double backoffFactor, long maxSleepMs) throws TimeoutException, InterruptedException {
    long startTimeMs = System.currentTimeMillis();
    long endTimeMs = startTimeMs + assertTimeoutMs;
    long currentSleepMs = 0;
    boolean done = false;
    try {
      while (!done && System.currentTimeMillis() < endTimeMs) {
        done = condition.apply(null);
        if (!done) {
          currentSleepMs = computeRetrySleep(currentSleepMs, backoffFactor, maxSleepMs, endTimeMs);
          log.debug("Condition check for '" + message + "' failed; sleeping for " + currentSleepMs + "ms");
          Thread.sleep(currentSleepMs);
        }
      }
      //one last try
      if (!done && !condition.apply(null)) {
        throw new TimeoutException("Timeout waiting for condition '" + message + "'.");
      }
    } catch (TimeoutException | InterruptedException e) {
      //pass through
      throw e;
    } catch (RuntimeException e) {
      throw new RuntimeException("Exception checking condition '" + message + "':" + e, e);
    }
  }

  /**
   * Computes a new sleep for a retry of a condition check.
   *
   * @param currentSleepMs    the last sleep duration in milliseconds
   * @param backoffFactor     the factor to multiple currentSleepMs
   * @param maxSleepMs        the maximum allowed sleep in milliseconds
   * @param endTimeMs         the end time based on timeout for the condition to become true
   * @return the new sleep time which will not exceed maxSleepMs and will also not cause to
   *         overshoot endTimeMs
   */
  public static long computeRetrySleep(long currentSleepMs, double backoffFactor, long maxSleepMs, long endTimeMs) {
    long newSleepMs = Math.round(currentSleepMs * backoffFactor);
    if (newSleepMs <= currentSleepMs) {
      // Prevent infinite loops
      newSleepMs = currentSleepMs + 1;
    }
    long currentTimeMs = System.currentTimeMillis();
    newSleepMs = Math.min(Math.min(newSleepMs, maxSleepMs), endTimeMs - currentTimeMs);
    return newSleepMs;
  }

}
