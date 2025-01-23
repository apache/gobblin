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

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;


/** Tests for impl of {@link ActivityTimeoutStrategy}*/
public class ActivityTimeoutStrategyTest {

  @Test
  public void testGenerateWorkunitsActivityTimeoutStrategy() {
    ActivityTimeoutStrategy.GenerateWorkunitsActivityTimeoutStrategy strategy =
        new ActivityTimeoutStrategy.GenerateWorkunitsActivityTimeoutStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityTimeoutStrategy.DEFAULT_GENERATE_WORKUNITS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.GENERATE_WORKUNITS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "10");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(10));
  }

  @Test
  public void testRecommendScalingActivityTimeoutStrategy() {
    ActivityTimeoutStrategy.RecommendScalingActivityTimeoutStrategy strategy =
        new ActivityTimeoutStrategy.RecommendScalingActivityTimeoutStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityTimeoutStrategy.DEFAULT_RECOMMEND_SCALING_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.RECOMMEND_SCALING_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "13");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(13));
  }

  @Test
  public void testDeleteWorkDirsActivityTimeoutStrategy() {
    ActivityTimeoutStrategy.DeleteWorkDirsActivityTimeoutStrategy strategy =
        new ActivityTimeoutStrategy.DeleteWorkDirsActivityTimeoutStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityTimeoutStrategy.DEFAULT_DELETE_WORK_DIRS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.DELETE_WORK_DIRS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "27");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(27));
  }

  @Test
  public void testProcessWorkunitActivityTimeoutStrategy() {
    ActivityTimeoutStrategy.ProcessWorkunitActivityTimeoutStrategy strategy =
        new ActivityTimeoutStrategy.ProcessWorkunitActivityTimeoutStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityTimeoutStrategy.DEFAULT_PROCESS_WORKUNIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.PROCESS_WORKUNIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "33");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(33));
  }

  @Test
  public void testCommitActivityTimeoutStrategy() {
    ActivityTimeoutStrategy.CommitActivityTimeoutStrategy strategy =
        new ActivityTimeoutStrategy.CommitActivityTimeoutStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityTimeoutStrategy.DEFAULT_COMMIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.COMMIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "33");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(33));
  }
}
