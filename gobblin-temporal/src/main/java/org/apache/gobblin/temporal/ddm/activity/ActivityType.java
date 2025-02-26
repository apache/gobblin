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
import java.util.Properties;

import lombok.Getter;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.util.PropertiesUtils;


/**
 * Enum representing different types of activities in the Temporal workflow.
 * Each activity type corresponds to a specific operation that can be performed.
 */
public enum ActivityType {
  /** Activity type for generating work units. */
  GENERATE_WORKUNITS(GobblinTemporalConfigurationKeys.TEMPORAL_GENERATE_WORKUNITS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES),

  /** Activity type for recommending scaling operations. */
  RECOMMEND_SCALING(GobblinTemporalConfigurationKeys.TEMPORAL_RECOMMEND_SCALING_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES),

  /** Activity type for deleting work directories. */
  DELETE_WORK_DIRS(GobblinTemporalConfigurationKeys.TEMPORAL_DELETE_WORK_DIRS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES),

  /** Activity type for processing a work unit. */
  PROCESS_WORKUNIT(GobblinTemporalConfigurationKeys.TEMPORAL_PROCESS_WORKUNIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES),

  /** Activity type for committing step. */
  COMMIT(GobblinTemporalConfigurationKeys.TEMPORAL_COMMIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES),

  /** Activity type for submitting GTE. */
  SUBMIT_GTE(GobblinTemporalConfigurationKeys.TEMPORAL_SUBMIT_GTE_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES),

  /** Default placeholder activity type. */
  DEFAULT_ACTIVITY(GobblinTemporalConfigurationKeys.ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES);

  @Getter private final String startToCloseTimeoutConfigKey;

  ActivityType(String startToCloseTimeoutConfigKey) {
    this.startToCloseTimeoutConfigKey = startToCloseTimeoutConfigKey;
  }

  public ActivityOptions buildActivityOptions(Properties props) {
    return ActivityOptions.newBuilder()
        .setStartToCloseTimeout(getStartToCloseTimeout(props))
        .setHeartbeatTimeout(getHeartbeatTimeout(props))
        .setRetryOptions(buildRetryOptions(props))
        .build();
  }

  private Duration getStartToCloseTimeout(Properties props) {
    return Duration.ofMinutes(PropertiesUtils.getPropAsInt(props, this.startToCloseTimeoutConfigKey,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
  }

  private Duration getHeartbeatTimeout(Properties props) {
    return Duration.ofMinutes(PropertiesUtils.getPropAsInt(props,
        GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_HEARTBEAT_TIMEOUT_MINUTES,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_HEARTBEAT_TIMEOUT_MINUTES));
  }

  private RetryOptions buildRetryOptions(Properties props) {
    int maximumIntervalSeconds = PropertiesUtils.getPropAsInt(props,
        GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_INTERVAL_SECONDS,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_INTERVAL_SECONDS);

    int initialIntervalSeconds = Math.min(PropertiesUtils.getPropAsInt(props,
        GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_INITIAL_INTERVAL_SECONDS,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_INITIAL_INTERVAL_SECONDS),
        maximumIntervalSeconds);

    return RetryOptions.newBuilder()
        .setInitialInterval(Duration.ofSeconds(initialIntervalSeconds))
        .setMaximumInterval(Duration.ofSeconds(maximumIntervalSeconds))
        .setBackoffCoefficient(PropertiesUtils.getPropAsDouble(props,
            GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_BACKOFF_COEFFICIENT,
            GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_BACKOFF_COEFFICIENT))
        .setMaximumAttempts(PropertiesUtils.getPropAsInt(props,
            GobblinTemporalConfigurationKeys.TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_ATTEMPTS,
            GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_ACTIVITY_RETRY_OPTIONS_MAXIMUM_ATTEMPTS))
        .build();
  }
}
