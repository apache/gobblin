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
package gobblin.retry;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.writer.exception.NonTransientException;

/**
 * Factory class that builds Retryer.
 * It's recommended to use with ConfigBuilder so that with State and with prefix of the config key,
 * user can easily instantiate Retryer.
 *
 * @see GoogleAnalyticsUnsampledExtractor for some examples.
 *
 * @param <T>
 */
public class RetryerFactory<T> {
  private static final Logger LOG = LoggerFactory.getLogger(RetryerFactory.class);
  public static final String RETRY_MULTIPLIER = "multiplier";
  public static final String RETRY_INTERVAL_MS = "interval_ms";
  public static final String RETRY_TIME_OUT_MS = "time_out_ms";
  public static final String RETRY_TYPE = "retry_type";

  private static final Predicate<Throwable> RETRY_EXCEPTION_PREDICATE;
  private static final Config DEFAULTS;
  static {
    RETRY_EXCEPTION_PREDICATE = new Predicate<Throwable>() {
      @Override
      public boolean apply(Throwable t) {
        return !(t instanceof NonTransientException);
      }
    };

    Map<String, Object> configMap = ImmutableMap.<String, Object>builder()
                                                .put(RETRY_TIME_OUT_MS, TimeUnit.MINUTES.toMillis(5L))
                                                .put(RETRY_INTERVAL_MS, TimeUnit.SECONDS.toMillis(30L))
                                                .put(RETRY_MULTIPLIER, 2L)
                                                .put(RETRY_TYPE, RetryType.EXPONENTIAL.name())
                                                .build();
    DEFAULTS = ConfigFactory.parseMap(configMap);
  }

  public static enum RetryType {
    EXPONENTIAL,
    FIXED;
  }

  /**
   * Creates new instance of retryer based on the config.
   * Accepted config keys are defined in RetryerFactory as static member variable.
   * You can use State along with ConfigBuilder and config prefix to build config.
   *
   * @param config
   * @return
   */
  public static <T> Retryer<T> newInstance(Config config) {
    config = config.withFallback(DEFAULTS);
    RetryType type = RetryType.valueOf(config.getString(RETRY_TYPE).toUpperCase());

    switch (type) {
      case EXPONENTIAL:
        return newExponentialRetryer(config);
      case FIXED:
        return newFixedRetryer(config);
      default:
        throw new IllegalArgumentException(type + " is not supported");
    }
  }

  private static <T> Retryer<T> newFixedRetryer(Config config) {
    return RetryerBuilder.<T> newBuilder()
        .retryIfException(RETRY_EXCEPTION_PREDICATE)
        .withWaitStrategy(WaitStrategies.fixedWait(config.getLong(RETRY_INTERVAL_MS), TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(config.getLong(RETRY_TIME_OUT_MS), TimeUnit.MILLISECONDS))
        .build();
  }

  private static <T> Retryer<T> newExponentialRetryer(Config config) {

    return RetryerBuilder.<T> newBuilder()
        .retryIfException(RETRY_EXCEPTION_PREDICATE)
        .withWaitStrategy(WaitStrategies.exponentialWait(config.getLong(RETRY_MULTIPLIER),
                                                         config.getLong(RETRY_INTERVAL_MS),
                                                         TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterDelay(config.getLong(RETRY_TIME_OUT_MS), TimeUnit.MILLISECONDS))
        .build();
  }
}
