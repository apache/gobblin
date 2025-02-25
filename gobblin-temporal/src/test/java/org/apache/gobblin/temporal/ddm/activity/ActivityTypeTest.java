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

package org.apache.gobblin.temporal.ddm.activity;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import io.temporal.activity.ActivityOptions;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;


/** Tests for {@link ActivityType} */
public class ActivityTypeTest {

  private Properties props;
  private final List<ActivityType> activityTypes = Arrays.asList(ActivityType.values());

  @BeforeMethod
  public void setUp() {
    props = new Properties();
  }

  @Test
  public void testDefaultValuesForTimeouts() {
    activityTypes.stream().map(activityType -> activityType.buildActivityOptions(props)).forEach(activityOptions -> {
      Assert.assertEquals(activityOptions.getStartToCloseTimeout(),
          Duration.ofMinutes(GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
      Assert.assertEquals(activityOptions.getHeartbeatTimeout(),
          Duration.ofMinutes(GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_HEARTBEAT_TIMEOUT_MINUTES));
      Assert.assertEquals(activityOptions.getRetryOptions().getInitialInterval(),
          Duration.ofSeconds(GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_INITIAL_INTERVAL_SECONDS));
      Assert.assertEquals(activityOptions.getRetryOptions().getMaximumInterval(),
          Duration.ofSeconds(GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_INTERVAL_SECONDS));
      Assert.assertEquals(activityOptions.getRetryOptions().getBackoffCoefficient(),
          GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_BACKOFF_COEFFICIENT, 0.01);
      Assert.assertEquals(activityOptions.getRetryOptions().getMaximumAttempts(),
          GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_ATTEMPTS);
    });
  }

  @DataProvider(name = "activityTypesWithStartToCloseTimeout")
  public Object[][] activityTypesWithStartToCloseTimeout() {
    return new Object[][] {
        {ActivityType.GENERATE_WORKUNITS, 333},
        {ActivityType.RECOMMEND_SCALING, 111},
        {ActivityType.DELETE_WORK_DIRS, 222},
        {ActivityType.PROCESS_WORKUNIT, 555},
        {ActivityType.COMMIT, 444},
        {ActivityType.DEFAULT_ACTIVITY, 1}
    };
  }

  @Test(dataProvider = "activityTypesWithStartToCloseTimeout")
  public void testStartToCloseTimeout(ActivityType activityType, int expectedTimeout) {
    props.setProperty(activityType.getStartToCloseTimeoutConfigKey(), Integer.toString(expectedTimeout));
    Assert.assertEquals(activityType.buildActivityOptions(props).getStartToCloseTimeout(), Duration.ofMinutes(expectedTimeout));
  }

  @Test
  public void testHeartBeatTimeout() {
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_HEARTBEAT_TIMEOUT_MINUTES, "14");
    activityTypes.stream().map(activityType -> activityType.buildActivityOptions(props)).forEach(activityOptions -> {
      Assert.assertEquals(activityOptions.getHeartbeatTimeout(), Duration.ofMinutes(14));
    });
  }

  @Test
  public void testRetryOptions() {
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_INITIAL_INTERVAL_SECONDS, "115");
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_INTERVAL_SECONDS, "5550");
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_BACKOFF_COEFFICIENT, "7.0");
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_ATTEMPTS, "21");

    activityTypes.stream().map(activityType -> activityType.buildActivityOptions(props)).forEach(activityOptions -> {
      Assert.assertEquals(activityOptions.getRetryOptions().getInitialInterval(), Duration.ofSeconds(115));
      Assert.assertEquals(activityOptions.getRetryOptions().getMaximumInterval(), Duration.ofSeconds(5550));
      Assert.assertEquals(activityOptions.getRetryOptions().getBackoffCoefficient(), 7.0, 0.01);
      Assert.assertEquals(activityOptions.getRetryOptions().getMaximumAttempts(), 21);
    });
  }

  @Test(dataProvider = "activityTypesWithStartToCloseTimeout")
  public void testBuildActivityOptions(ActivityType activityType, int expectedTimeout) {
    props.setProperty(activityType.getStartToCloseTimeoutConfigKey(), Integer.toString(expectedTimeout));
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_HEARTBEAT_TIMEOUT_MINUTES, "144");
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_INITIAL_INTERVAL_SECONDS, "115");
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_INTERVAL_SECONDS, "5550");
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_BACKOFF_COEFFICIENT, "7.0");
    props.setProperty(GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_ATTEMPTS, "21");

    ActivityOptions activityOptions = activityType.buildActivityOptions(props);

    Assert.assertEquals(activityOptions.getStartToCloseTimeout(), Duration.ofMinutes(expectedTimeout));
    Assert.assertEquals(activityOptions.getHeartbeatTimeout(), Duration.ofMinutes(144));
    Assert.assertEquals(activityOptions.getRetryOptions().getInitialInterval(), Duration.ofSeconds(115));
    Assert.assertEquals(activityOptions.getRetryOptions().getMaximumInterval(), Duration.ofSeconds(5550));
    Assert.assertEquals(activityOptions.getRetryOptions().getBackoffCoefficient(), 7.0, 0.01);
    Assert.assertEquals(activityOptions.getRetryOptions().getMaximumAttempts(), 21);
  }

}
