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
package org.apache.gobblin.util.retry;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.gobblin.util.ConfigUtils;


import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.google.common.annotations.VisibleForTesting;

import org.apache.gobblin.exception.NonTransientException;

/**
 * Factory class that builds Retryer.
 * It's recommended to use with ConfigBuilder so that with State and with prefix of the config key,
 * user can easily instantiate Retryer.
 *
 * see GoogleAnalyticsUnsampledExtractor for some examples.
 *
 * @param <T>
 */
public class RetryerFactory<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RetryerFactory.class);
  public static final String RETRY_MULTIPLIER = "multiplier";
  public static final String RETRY_INTERVAL_MS = "interval_ms";
  public static final String RETRY_TIME_OUT_MS = "time_out_ms";
  public static final String RETRY_TYPE = "retry_type";
  // value large or equal to 1
  public static final String RETRY_TIMES = "retry_times";

  // Key to store a comma-separated list of exception class names that should be retried
  public static final String EXCEPTION_LIST_FOR_RETRY_CONFIG_KEY = "EXCEPTION_LIST_FOR_RETRY";
  public static final Predicate<Throwable> RETRY_EXCEPTION_PREDICATE_DEFAULT;

  private static final Config DEFAULTS;

  static {
    RETRY_EXCEPTION_PREDICATE_DEFAULT = t -> !(t instanceof NonTransientException);

    Map<String, Object> configMap = ImmutableMap.<String, Object>builder()
                                                .put(RETRY_TIME_OUT_MS, TimeUnit.MINUTES.toMillis(5L))
                                                .put(RETRY_INTERVAL_MS, TimeUnit.SECONDS.toMillis(30L))
                                                .put(RETRY_MULTIPLIER, TimeUnit.SECONDS.toMillis(1L))
                                                .put(RETRY_TYPE, RetryType.EXPONENTIAL.name())
                                                .put(RETRY_TIMES, 2)
                                                .build();
    DEFAULTS = ConfigFactory.parseMap(configMap);
  }

  public static enum RetryType {
    EXPONENTIAL,
    FIXED,
    FIXED_ATTEMPT;
  }

  /**
   * Creates new instance of retryer based on the config.
   * Accepted config keys are defined in RetryerFactory as static member variable.
   * You can use State along with ConfigBuilder and config prefix to build config.
   *
   * @param config
   * @param optRetryListener e.g. for logging failures
   */
  public static <T> Retryer<T> newInstance(Config config, Optional<RetryListener> optRetryListener) {
    config = config.withFallback(DEFAULTS);
    RetryType type = RetryType.valueOf(config.getString(RETRY_TYPE).toUpperCase());

    RetryerBuilder<T> builder;

    Predicate<Throwable> retryPredicate = getRetryPredicateFromConfigOrDefault(config);

    switch (type) {
      case EXPONENTIAL:
        builder = newExponentialRetryerBuilder(config, retryPredicate);
        break;
      case FIXED:
        builder = newFixedRetryerBuilder(config, retryPredicate);
        break;
      case FIXED_ATTEMPT:
        builder = newFixedAttemptBoundRetryerBuilder(config, retryPredicate);
        break;
      default:
        throw new IllegalArgumentException(type + " is not supported");
    }
    optRetryListener.ifPresent(builder::withRetryListener);
    return builder.build();
  }

  /**
   Retrieves a retry predicate based on the configuration provided. If the configuration
   does not specify any exceptions, a default retry predicate is returned.
   *
   @param config the configuration object containing the list of exception class names
   @return a Predicate that evaluates to true if the throwable should be retried, false otherwise
   */
  @VisibleForTesting
  public static Predicate<Throwable> getRetryPredicateFromConfigOrDefault(Config config) {
    // Retrieve the list of exception class names from the configuration
    List<String> exceptionList = ConfigUtils.getStringList(config, EXCEPTION_LIST_FOR_RETRY_CONFIG_KEY);

    // If the exception list is null or empty, return the default retry predicate
    if (exceptionList == null || exceptionList.isEmpty()) {
      return RETRY_EXCEPTION_PREDICATE_DEFAULT;
    }

    // Create a retry predicate by mapping each exception class name to a Predicate
    Predicate<Throwable> retryPredicate = exceptionList.stream().map(exceptionClassName -> {
          try {
            Class<?> clazz = Class.forName(exceptionClassName);
            if (Exception.class.isAssignableFrom(clazz)) {
              // Return a Predicate that checks if a Throwable is an instance of the class
              return (Predicate<Throwable>) clazz::isInstance;
            } else {
              LOG.error("{} is not an exception.", exceptionClassName);
            }
          } catch (ClassNotFoundException exception) {
            LOG.error("Class not found for the given exception className {}", exceptionClassName, exception);
          } catch (Exception exception) {
            LOG.error("Failed to instantiate exception {}", exceptionClassName, exception);
          }
          return null;
        }).filter(Objects::nonNull) // Filter out any null values
        .reduce(com.google.common.base.Predicates::or) // Combine all predicates using logical OR
        .orElse(RETRY_EXCEPTION_PREDICATE_DEFAULT); // Default to retryExceptionPredicate if no valid predicates are found

    return retryPredicate;
  }

  /**
   * Creates new instance of retryer based on the config and having no {@link RetryListener}
   */
  public static <T> Retryer<T> newInstance(Config config) {
    return newInstance(config, Optional.empty());
  }

  private static <T> RetryerBuilder<T> newFixedRetryerBuilder(Config config,
      Predicate<Throwable> retryExceptionPredicate) {
    return RetryerBuilder.<T>newBuilder()
        .retryIfException(retryExceptionPredicate)
        .withWaitStrategy(WaitStrategies.fixedWait(config.getLong(RETRY_INTERVAL_MS), TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(config.getLong(RETRY_TIME_OUT_MS), TimeUnit.MILLISECONDS));
  }

  private static <T> RetryerBuilder<T> newExponentialRetryerBuilder(Config config,
      Predicate<Throwable> retryExceptionPredicate) {
    return RetryerBuilder.<T>newBuilder()
        .retryIfException(retryExceptionPredicate)
        .withWaitStrategy(
            WaitStrategies.exponentialWait(config.getLong(RETRY_MULTIPLIER), config.getLong(RETRY_INTERVAL_MS),
                TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(config.getLong(RETRY_TIME_OUT_MS), TimeUnit.MILLISECONDS));
  }

  private static <T> RetryerBuilder<T> newFixedAttemptBoundRetryerBuilder(Config config,
      Predicate<Throwable> retryExceptionPredicate) {
    return RetryerBuilder.<T>newBuilder()
        .retryIfException(retryExceptionPredicate)
        .withWaitStrategy(WaitStrategies.fixedWait(config.getLong(RETRY_INTERVAL_MS), TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(config.getInt(RETRY_TIMES)));
  }
}
