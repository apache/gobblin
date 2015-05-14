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
 * A factory class for {@link Throttler}s.
 *
 * @author ynli
 */
public class ThrottlerFactory {

  /**
   * Create a new {@link Throttler} instance.
   *
   * @param state a {@link State} instance carrying configuration properties
   * @return a new {@link Throttler} instance
   */
  public static Throttler newThrottler(State state) {
    Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_THROTTLING_TYPE_KEY),
        String.format("Missing configuration property %s for the throttler type",
            ConfigurationKeys.EXTRACT_THROTTLING_TYPE_KEY));
    Throttler.Type type = Throttler.Type.forName(state.getProp(ConfigurationKeys.EXTRACT_THROTTLING_TYPE_KEY));

    switch (type) {
      case RATE_BASED:
        Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_THROTTLING_RATE_LIMIT_KEY));
        int rateLimit = Integer.parseInt(state.getProp(ConfigurationKeys.EXTRACT_THROTTLING_RATE_LIMIT_KEY));
        TimeUnit rateTimeUnit = TimeUnit.valueOf(state
                .getProp(ConfigurationKeys.EXTRACT_THROTTLING_RATE_LIMIT_TIMEUNIT_KEY,
                    ConfigurationKeys.DEFAULT_EXTRACT_THROTTLING_RATE_LIMIT_TIMEUNIT).toUpperCase());
        return new RateBasedThrottler(rateLimit, rateTimeUnit);
      case TIME_BASED:
        Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_THROTTLING_TIME_LIMIT_KEY));
        long timeLimit = Long.parseLong(state.getProp(ConfigurationKeys.EXTRACT_THROTTLING_TIME_LIMIT_KEY));
        TimeUnit timeTimeUnit = TimeUnit.valueOf(state
                .getProp(ConfigurationKeys.EXTRACT_THROTTLING_TIME_LIMIT_TIMEUNIT_KEY,
                    ConfigurationKeys.DEFAULT_EXTRACT_THROTTLING_TIME_LIMIT_TIMEUNIT).toUpperCase());
        return new TimeBasedThrottler(timeLimit, timeTimeUnit);
      case COUNT_BASED:
        Preconditions.checkArgument(state.contains(ConfigurationKeys.EXTRACT_THROTTLING_COUNT_LIMIT_KEY));
        long countLimit = Long.parseLong(state.getProp(ConfigurationKeys.EXTRACT_THROTTLING_COUNT_LIMIT_KEY));
        return new CountBasedThrottler(countLimit);
      default:
        throw new IllegalArgumentException("Unrecognized throttler type: " + type.toString());
    }
  }
}
