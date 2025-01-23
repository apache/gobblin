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

import lombok.experimental.UtilityClass;

import org.apache.gobblin.temporal.ddm.activity.ActivityTimeoutStrategy;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;


/**
 * Utility class for handling Temporal timeout configurations.
 * This utility class provides methods to handle timeout configurations for different Temporal activities.
 * It uses a strategy pattern to determine the timeout for each activity type.
 */
@UtilityClass
public class TemporalTimeoutUtils {

  private static final Map<ActivityType, ActivityTimeoutStrategy> activityTimeoutStrategies = new HashMap<>();

  static {
    activityTimeoutStrategies.put(ActivityType.GENERATE_WORKUNITS, new ActivityTimeoutStrategy.GenerateWorkunitsActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.RECOMMEND_SCALING, new ActivityTimeoutStrategy.RecommendScalingActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.DELETE_WORK_DIRS, new ActivityTimeoutStrategy.DeleteWorkDirsActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.PROCESS_WORKUNIT, new ActivityTimeoutStrategy.ProcessWorkunitActivityTimeoutStrategy());
    activityTimeoutStrategies.put(ActivityType.COMMIT, new ActivityTimeoutStrategy.CommitActivityTimeoutStrategy());
  }

  /**
   * Retrieves the start to close timeout duration for a given {@link ActivityType} based on the provided properties.
   *
   * @param activityType the type of the activity for which the start to close timeout is being retrieved.
   * @param props the properties to be used for configuring the timeout.
   * @return the start to close timeout duration for the specified activity type.
   */
  public static Duration getStartToCloseTimeout(ActivityType activityType, Properties props) {
    ActivityTimeoutStrategy activityTimeoutStrategy = activityTimeoutStrategies.get(activityType);
    if (activityTimeoutStrategy == null) {
      return ActivityTimeoutStrategy.defaultStartToCloseTimeout;
    }
    return activityTimeoutStrategy.getStartToCloseTimeout(props);
  }

}