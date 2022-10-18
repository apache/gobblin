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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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

  private static final Predicate<Throwable> RETRY_EXCEPTION_PREDICATE;
  private static final Config DEFAULTS;
  static {
    RETRY_EXCEPTION_PREDICATE = t -> !(t instanceof NonTransientException);

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
    switch (type) {
      case EXPONENTIAL:
        builder = newExponentialRetryerBuilder(config);
        break;
      case FIXED:
        builder = newFixedRetryerBuilder(config);
        break;
      case FIXED_ATTEMPT:
        builder = newFixedAttemptBoundRetryerBuilder(config);
        break;
      default:
        throw new IllegalArgumentException(type + " is not supported");
    }
    optRetryListener.ifPresent(builder::withRetryListener);
    return builder.build();
  }

  /**
   * Creates new instance of retryer based on the config and having no {@link RetryListener}
   */
  public static <T> Retryer<T> newInstance(Config config) {
    return newInstance(config, Optional.empty());
  }

  private static <T> RetryerBuilder<T> newFixedRetryerBuilder(Config config) {
    return RetryerBuilder.<T> newBuilder()
        .retryIfException(RETRY_EXCEPTION_PREDICATE)
        .withWaitStrategy(WaitStrategies.fixedWait(config.getLong(RETRY_INTERVAL_MS), TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(config.getLong(RETRY_TIME_OUT_MS), TimeUnit.MILLISECONDS));
  }

  private static <T> RetryerBuilder<T> newExponentialRetryerBuilder(Config config) {
    return RetryerBuilder.<T> newBuilder()
        .retryIfException(RETRY_EXCEPTION_PREDICATE)
        .withWaitStrategy(WaitStrategies.exponentialWait(config.getLong(RETRY_MULTIPLIER),
                                                         config.getLong(RETRY_INTERVAL_MS),
                                                         TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(config.getLong(RETRY_TIME_OUT_MS), TimeUnit.MILLISECONDS));
  }

  private static <T> RetryerBuilder<T> newFixedAttemptBoundRetryerBuilder(Config config) {
    return RetryerBuilder.<T> newBuilder()
        .retryIfException(RETRY_EXCEPTION_PREDICATE)
        .withWaitStrategy(WaitStrategies.fixedWait(config.getLong(RETRY_INTERVAL_MS), TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(config.getInt(RETRY_TIMES)));
  }
}
