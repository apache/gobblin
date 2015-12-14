package gobblin.testing;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;

import com.google.common.base.Optional;

/** Common utilities to help with testing */
public class TestingUtils {
  public static final Double DEFAULT_ASSERT_BACKOFF_FACTOR = 1.5;
  public static final Long DEFAULT_MAX_ASSERT_BACKOFF_SLEEP_MS = 10 * 1000L;

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
  public static void assertWithBackoff(
      Callable<Boolean> condition, long assertTimeoutMs, String message,
      Logger log) throws TimeoutException, InterruptedException {
    assertWithBackoff(condition, assertTimeoutMs, Optional.of(message), Optional.of(log),
        Optional.<Double>absent(),
        Optional.<Long>absent());
  }

  /**
   * Performs a check for a condition with a back-off. Primary use of this is when a tests
   * needs for something asynchronous to happen, say a service to start.
   *
   * @param condition           the condition to wait for
   * @param assertTimeoutMs     the max time in milliseconds to wait for the condition to become true
   * @param message             a message to print while waiting for the condition
   * @throws TimeoutException   if the condition did not become true in the specified time budget
   */
  public static void assertWithBackoff(
      Callable<Boolean> condition, long assertTimeoutMs, String message)
      throws TimeoutException, InterruptedException {
    assertWithBackoff(condition, assertTimeoutMs, Optional.of(message), Optional.<Logger>absent(),
        Optional.<Double>absent(),
        Optional.<Long>absent());
  }

  /**
   * Performs a check for a condition with a back-off. Primary use of this is when a tests
   * needs for something asynchronous to happen, say a service to start.
   *
   * @param condition           the condition to wait for
   * @param assertTimeoutMs     the max time in milliseconds to wait for the condition to become true
   * @param message             an optional message to print while waiting for the condition
   * @param log                 an optional logger to use for logging waiting, results
   * @param backoffFactor       optional number to multiple the sleep after condition failure;
   *                            default is {@link #DEFAULT_ASSERT_BACKOFF_FACTOR}
   * @param maxSleepMs          the max time to sleep between condition failures; default is
   * @throws TimeoutException   if the condition did not become true in the specified time budget
   */
  public static void assertWithBackoff(
      Callable<Boolean> condition, long assertTimeoutMs, Optional<String> message,
      Optional<Logger> log, Optional<Double> backoffFactor, Optional<Long> maxSleepMs)
          throws TimeoutException, InterruptedException {
    long startTimeMs = System.currentTimeMillis();
    long endTimeMs = startTimeMs + assertTimeoutMs;
    double realBackoffFactor = backoffFactor.or(DEFAULT_ASSERT_BACKOFF_FACTOR);
    long realMaxSleepMs = maxSleepMs.or(DEFAULT_MAX_ASSERT_BACKOFF_SLEEP_MS);
    long currentSleepMs = 0;
    String realMessage = "'" + message.or("") + "'";
    boolean done = false;
    try {
      while (!done && System.currentTimeMillis() < endTimeMs) {
          done = condition.call().booleanValue();
          if (!done) {
            currentSleepMs = computeRetrySleep(currentSleepMs, realBackoffFactor, realMaxSleepMs,
                endTimeMs);
            if (log.isPresent()) {
              log.get().debug("Condition check for " + realMessage + " failed; sleeping for " +
                              currentSleepMs + "ms");
            }
            Thread.sleep(currentSleepMs);
          }
      }
      //one last try
      if (!done && !condition.call().booleanValue()) {
        throw new TimeoutException("Timeout waiting for condition " + realMessage);
      }
    } catch (TimeoutException|InterruptedException e) {
      //pass through
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Exception checking condition " + realMessage + ":" + e, e);
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
  public static long computeRetrySleep(long currentSleepMs, double backoffFactor, long maxSleepMs,
                                       long endTimeMs) {
    long newSleepMs = Math.round(currentSleepMs * backoffFactor);
    if (newSleepMs == currentSleepMs) {
      //prevent infinite loops
      ++newSleepMs;
    }
    long currentTimeMs = System.currentTimeMillis();
    newSleepMs = Math.min(Math.min(newSleepMs, maxSleepMs), endTimeMs - currentTimeMs);
    return newSleepMs;
  }

}
