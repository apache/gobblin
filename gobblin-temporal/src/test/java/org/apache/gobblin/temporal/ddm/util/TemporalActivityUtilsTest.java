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

package org.apache.gobblin.temporal.ddm.util;

import java.time.Duration;
import java.util.Properties;

import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;

import org.apache.gobblin.temporal.ddm.activity.ActivityType;


/** Tests for {@link TemporalActivityUtils} */
public class TemporalActivityUtilsTest {

  @Test
  public void testBuildActivityOptions() {
    ActivityType activityType = Mockito.mock(ActivityType.class);
    Properties props = Mockito.mock(Properties.class);

    Duration timeout = Duration.ofMinutes(30);
    RetryOptions retryOptions = RetryOptions.newBuilder().setMaximumAttempts(3).build();

    MockedStatic<TemporalTimeoutUtils> mockedTemporalTimeoutUtils = Mockito.mockStatic(TemporalTimeoutUtils.class);
    MockedStatic<TemporalRetryUtils> mockedTemporalRetryUtils = Mockito.mockStatic(TemporalRetryUtils.class);

    mockedTemporalTimeoutUtils.when(() -> TemporalTimeoutUtils.getStartToCloseTimeout(activityType, props)).thenReturn(timeout);
    mockedTemporalRetryUtils.when(() -> TemporalRetryUtils.getRetryOptions(activityType, props)).thenReturn(retryOptions);

    ActivityOptions activityOptions = TemporalActivityUtils.buildActivityOptions(activityType, props);

    Assert.assertEquals(timeout, activityOptions.getStartToCloseTimeout());
    Assert.assertEquals(retryOptions, activityOptions.getRetryOptions());

    mockedTemporalTimeoutUtils.close();
    mockedTemporalRetryUtils.close();
  }
}
