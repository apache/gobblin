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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.temporal.ddm.activity.ActivityConfigurationStrategy;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;


/** Utility class for handling Temporal Activity related operations. */
@UtilityClass
@Slf4j
public class TemporalActivityUtils {

  @VisibleForTesting
  protected static final RetryOptions DEFAULT_RETRY_OPTIONS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  private static final Map<ActivityType, ActivityConfigurationStrategy> activityConfigurationStrategies = new HashMap<>();

  static {
    activityConfigurationStrategies.put(ActivityType.GENERATE_WORKUNITS, new ActivityConfigurationStrategy.GenerateWorkunitsActivityConfigurationStrategy());
    activityConfigurationStrategies.put(ActivityType.RECOMMEND_SCALING, new ActivityConfigurationStrategy.RecommendScalingActivityConfigurationStrategy());
    activityConfigurationStrategies.put(ActivityType.DELETE_WORK_DIRS, new ActivityConfigurationStrategy.DeleteWorkDirsActivityConfigurationStrategy());
    activityConfigurationStrategies.put(ActivityType.PROCESS_WORKUNIT, new ActivityConfigurationStrategy.ProcessWorkunitActivityConfigurationStrategy());
    activityConfigurationStrategies.put(ActivityType.COMMIT, new ActivityConfigurationStrategy.CommitActivityConfigurationStrategy());
  }

  /**
   * Builds and returns an {@link ActivityOptions} object configured with the specified {@link ActivityType} and properties.
   *
   * @param activityType the type of the activity for which the options are being built.
   * @param props the properties to be used for configuring the activity options.
   * @return an {@link ActivityOptions} object configured with the specified activity type and properties.
   */
  public static ActivityOptions buildActivityOptions(ActivityType activityType, Properties props) {
    return ActivityOptions.newBuilder()
        .setStartToCloseTimeout(getStartToCloseTimeout(activityType, props))
        .setRetryOptions(buildRetryOptions(activityType, props))
        .build();
  }

  /**
   * Retrieves the start to close timeout duration for a given {@link ActivityType} based on the provided properties.
   *
   * @param activityType the type of the activity for which the start to close timeout is being retrieved.
   * @param props the properties to be used for configuring the timeout.
   * @return the start to close timeout duration for the specified activity type.
   */
  private static Duration getStartToCloseTimeout(ActivityType activityType, Properties props) {
    ActivityConfigurationStrategy activityConfigurationStrategy = activityConfigurationStrategies.get(activityType);
    if (activityConfigurationStrategy == null) {
      log.warn("No configuration strategy found for activity type {}. Using default start to close timeout.", activityType);
      return ActivityConfigurationStrategy.defaultStartToCloseTimeout;
    }
    return activityConfigurationStrategy.getStartToCloseTimeout(props);
  }

  /**
   * Builds and returns an {@link RetryOptions} object configured with the specified {@link ActivityType} and properties.
   *
   * @param activityType the type of the activity for which the options are being built.
   * @param props the properties to be used for configuring the activity options.
   * @return an {@link RetryOptions} object configured with the specified activity type and properties.
   */
  private static RetryOptions buildRetryOptions(ActivityType activityType, Properties props) {
    // Currently returning just the default retry options for each activity type
    return DEFAULT_RETRY_OPTIONS;
  }

}
