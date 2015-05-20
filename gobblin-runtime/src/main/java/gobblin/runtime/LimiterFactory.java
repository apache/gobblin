/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;


/**
 * A factory class for {@link Limiter}s.
 *
 * @author ynli
 */
public class LimiterFactory {

  /**
   * Create a new {@link Limiter} instance.
   *
   * <p>
   *   Note this method will always return a new {@link Limiter} instance of one of the supported types defined
   *   in {@link Limiter.Type} as long as the input configuration specifies a supported {@link Limiter.Type}. It
   *   will throw an {@link IllegalArgumentException} if otherwise.
   * </p>
   *
   * @param state a {@link State} instance carrying configuration properties
   * @return a new {@link Limiter} instance
   * @throws IllegalArgumentException if the input configuration does not specify a valid supported
   */
  public static Limiter newLimiter(State state) {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_LIMIT_TYPE_KEY),
        String.format("Missing configuration property %s for the Limiter type",
            ConfigurationKeys.EXTRACT_LIMIT_TYPE_KEY));
    Limiter.Type type = Limiter.Type.forName(state.getProp(ConfigurationKeys.EXTRACT_LIMIT_TYPE_KEY));

    switch (type) {
      case RATE_BASED:
        Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_LIMIT_RATE_LIMIT_KEY));
        int rateLimit = Integer.parseInt(state.getProp(ConfigurationKeys.EXTRACT_LIMIT_RATE_LIMIT_KEY));
        if (state.contains(ConfigurationKeys.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY)) {
          TimeUnit rateTimeUnit = TimeUnit.valueOf(
              state.getProp(ConfigurationKeys.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY).toUpperCase());
          return new RateBasedLimiter(rateLimit, rateTimeUnit);
        }
        return new RateBasedLimiter(rateLimit);
      case TIME_BASED:
        Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_LIMIT_TIME_LIMIT_KEY));
        long timeLimit = Long.parseLong(state.getProp(ConfigurationKeys.EXTRACT_LIMIT_TIME_LIMIT_KEY));
        if (state.contains(ConfigurationKeys.EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY)) {
          TimeUnit timeTimeUnit = TimeUnit.valueOf(
              state.getProp(ConfigurationKeys.EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY).toUpperCase());
          return new TimeBasedLimiter(timeLimit, timeTimeUnit);
        }
        return new TimeBasedLimiter(timeLimit);
      case COUNT_BASED:
        Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_LIMIT_COUNT_LIMIT_KEY));
        long countLimit = Long.parseLong(state.getProp(ConfigurationKeys.EXTRACT_LIMIT_COUNT_LIMIT_KEY));
        return new CountBasedLimiter(countLimit);
      case POOL_BASED:
        Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_LIMIT_POOL_SIZE_KEY));
        int poolSize = Integer.parseInt(state.getProp(ConfigurationKeys.EXTRACT_LIMIT_POOL_SIZE_KEY));
        return new PoolBasedLimiter(poolSize);
      default:
        throw new IllegalArgumentException("Unrecognized Limiter type: " + type.toString());
    }
  }
}
