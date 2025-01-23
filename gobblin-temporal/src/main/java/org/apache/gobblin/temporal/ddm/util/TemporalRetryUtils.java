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
import io.temporal.common.RetryOptions;
import lombok.experimental.UtilityClass;
import org.apache.gobblin.temporal.ddm.activity.ActivityType;


/**
 * Utility class for handling Temporal Retry related operations.
 * This class provides methods to build and retrieve {@link RetryOptions} for different {@link ActivityType}s.
 */
@UtilityClass
public class TemporalRetryUtils {

  private static final RetryOptions DEFAULT_RETRY_OPTIONS = RetryOptions.newBuilder()
      .setInitialInterval(Duration.ofSeconds(3))
      .setMaximumInterval(Duration.ofSeconds(100))
      .setBackoffCoefficient(2)
      .setMaximumAttempts(4)
      .build();

  /**
   * Builds and returns an {@link RetryOptions} object configured with the specified {@link ActivityType} and properties.
   *
   * @param activityType the type of the activity for which the options are being built.
   * @param props the properties to be used for configuring the activity options.
   * @return an {@link RetryOptions} object configured with the specified activity type and properties.
   */
  public static RetryOptions getRetryOptions(ActivityType activityType, Properties props) {
    // Currently returning just the default retry options for each activity type
    return DEFAULT_RETRY_OPTIONS;
  }

}