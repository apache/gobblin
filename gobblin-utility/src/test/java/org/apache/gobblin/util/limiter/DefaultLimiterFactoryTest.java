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

package org.apache.gobblin.util.limiter;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;


/**
 * Unit tests for {@link DefaultLimiterFactory}.
 */
@Test(groups = {"gobblin.util.limiter"})
public class DefaultLimiterFactoryTest {

  @Test
  public void testNewLimiter() {
    State stateWithRateLimitDeprecatedKeys = new State();
    stateWithRateLimitDeprecatedKeys.setProp(DefaultLimiterFactory.EXTRACT_LIMIT_TYPE_KEY, BaseLimiterType.RATE_BASED);
    stateWithRateLimitDeprecatedKeys.setProp(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_KEY_DEP, "10");
    stateWithRateLimitDeprecatedKeys
        .setProp(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY_DEP, TimeUnit.MINUTES);
    Limiter rateLimiterFromDeprecatedKeys = DefaultLimiterFactory.newLimiter(stateWithRateLimitDeprecatedKeys);
    Assert.assertTrue(rateLimiterFromDeprecatedKeys instanceof RateBasedLimiter);
    Assert.assertTrue(stateWithRateLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_KEY));
    Assert
        .assertFalse(stateWithRateLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_KEY_DEP));
    Assert.assertTrue(
        stateWithRateLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY));
    Assert.assertFalse(
        stateWithRateLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY_DEP));

    State stateWithTimeLimitDeprecatedKeys = new State();
    stateWithTimeLimitDeprecatedKeys.setProp(DefaultLimiterFactory.EXTRACT_LIMIT_TYPE_KEY, BaseLimiterType.TIME_BASED);
    stateWithTimeLimitDeprecatedKeys.setProp(DefaultLimiterFactory.EXTRACT_LIMIT_TIME_LIMIT_KEY_DEP, "10");
    stateWithTimeLimitDeprecatedKeys
        .setProp(DefaultLimiterFactory.EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY_DEP, TimeUnit.MINUTES);
    Limiter timeLimiterFromDeprecatedKeys = DefaultLimiterFactory.newLimiter(stateWithTimeLimitDeprecatedKeys);
    Assert.assertTrue(timeLimiterFromDeprecatedKeys instanceof TimeBasedLimiter);
    Assert.assertTrue(stateWithTimeLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_TIME_LIMIT_KEY));
    Assert
        .assertFalse(stateWithTimeLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_TIME_LIMIT_KEY_DEP));
    Assert.assertTrue(
        stateWithTimeLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY));
    Assert.assertFalse(
        stateWithTimeLimitDeprecatedKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_TIME_LIMIT_TIMEUNIT_KEY_DEP));

    State stateWithNewKeys = new State();
    stateWithNewKeys.setProp(DefaultLimiterFactory.EXTRACT_LIMIT_TYPE_KEY, BaseLimiterType.RATE_BASED);
    stateWithNewKeys.setProp(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_KEY_DEP, "10");
    stateWithNewKeys.setProp(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY_DEP, TimeUnit.MINUTES);
    Limiter rateLimiterFromNewKeys = DefaultLimiterFactory.newLimiter(stateWithNewKeys);
    Assert.assertTrue(rateLimiterFromNewKeys instanceof RateBasedLimiter);

    Assert.assertTrue(stateWithNewKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_KEY));
    Assert.assertFalse(stateWithNewKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_KEY_DEP));
    Assert.assertTrue(stateWithNewKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY));
    Assert.assertFalse(stateWithNewKeys.contains(DefaultLimiterFactory.EXTRACT_LIMIT_RATE_LIMIT_TIMEUNIT_KEY_DEP));
  }
}
