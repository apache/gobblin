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

package gobblin.util.limiter;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import gobblin.configuration.State;


/**
 * A default factory class for {@link Limiter}s.
 *
 * @author ynli
 */
public class DefaultLimiterFactory {

  public static final String EXTRACT_LIMIT_TYPE_KEY = "extract.limit.type";
  public static final String EXTRACT_LIMIT_RATE_LIMIT_KEY = "extract.limit.rate.limit";
  public static final String EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY = "extract.limit.rate.limit.timeunit";
  public static final String EXTRACT_LIMIT_TIME_LIMIT_KEY = "extract.limit.time.limit";
  public static final String EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY = "extract.limit.time.limit.timeunit";
  public static final String EXTRACT_LIMIT_COUNT_LIMIT_KEY = "extract.limit.count.limit";
  public static final String EXTRACT_LIMIT_POOL_SIZE_KEY = "extract.limit.pool.size";

  /**
   * Create a new {@link Limiter} instance of one of the types in {@link BaseLimiterType}.
   *
   * <p>
   *   Note this method will always return a new {@link Limiter} instance of one of the supported types defined
   *   in {@link BaseLimiterType} as long as the input configuration specifies a supported {@link BaseLimiterType}.
   *   It will throw an {@link IllegalArgumentException} if none of the supported {@link BaseLimiterType}s is
   *   specified or if any of the required configuration properties for the specified {@link BaseLimiterType}
   *   is not present.
   * </p>
   *
   * <p>
   *   This method will return a functional {@link Limiter} if the configuration is correct. If instead, a
   *   {@link Limiter} is optional or the caller is fine with a {@link Limiter} that is not really limiting any
   *   events, then the caller should first make sure that the {@link Limiter} should indeed be created using
   *   this method, or handle the exception (if any is thrown) appropriately.
   * </p>
   *
   * @param state a {@link State} instance carrying configuration properties
   * @return a new {@link Limiter} instance
   * @throws IllegalArgumentException if the input configuration does not specify a valid supported
   */
  public static Limiter newLimiter(State state) {
    Preconditions.checkArgument(state.contains(EXTRACT_LIMIT_TYPE_KEY),
        String.format("Missing configuration property %s for the Limiter type", EXTRACT_LIMIT_TYPE_KEY));
    BaseLimiterType type = BaseLimiterType.forName(state.getProp(EXTRACT_LIMIT_TYPE_KEY));

    switch (type) {
      case RATE_BASED:
        Preconditions.checkArgument(state.contains(EXTRACT_LIMIT_RATE_LIMIT_KEY));
        int rateLimit = Integer.parseInt(state.getProp(EXTRACT_LIMIT_RATE_LIMIT_KEY));
        if (state.contains(EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY)) {
          TimeUnit rateTimeUnit = TimeUnit.valueOf(state.getProp(EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY).toUpperCase());
          return new RateBasedLimiter(rateLimit, rateTimeUnit);
        }
        return new RateBasedLimiter(rateLimit);
      case TIME_BASED:
        Preconditions.checkArgument(state.contains(EXTRACT_LIMIT_TIME_LIMIT_KEY));
        long timeLimit = Long.parseLong(state.getProp(EXTRACT_LIMIT_TIME_LIMIT_KEY));
        if (state.contains(EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY)) {
          TimeUnit timeTimeUnit = TimeUnit.valueOf(state.getProp(EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY).toUpperCase());
          return new TimeBasedLimiter(timeLimit, timeTimeUnit);
        }
        return new TimeBasedLimiter(timeLimit);
      case COUNT_BASED:
        Preconditions.checkArgument(state.contains(EXTRACT_LIMIT_COUNT_LIMIT_KEY));
        long countLimit = Long.parseLong(state.getProp(EXTRACT_LIMIT_COUNT_LIMIT_KEY));
        return new CountBasedLimiter(countLimit);
      case POOL_BASED:
        Preconditions.checkArgument(state.contains(EXTRACT_LIMIT_POOL_SIZE_KEY));
        int poolSize = Integer.parseInt(state.getProp(EXTRACT_LIMIT_POOL_SIZE_KEY));
        return new PoolBasedLimiter(poolSize);
      default:
        throw new IllegalArgumentException("Unrecognized Limiter type: " + type.toString());
    }
  }
}
