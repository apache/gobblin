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


/** Tests for impl of {@link ActivityConfigurationStrategy}*/
public class ActivityConfigurationStrategyTest {

  @Test
  public void testGenerateWorkunitsActivityConfigurationStrategy() {
    ActivityConfigurationStrategy.GenerateWorkunitsActivityConfigurationStrategy strategy =
        new ActivityConfigurationStrategy.GenerateWorkunitsActivityConfigurationStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityConfigurationStrategy.DEFAULT_GENERATE_WORKUNITS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.GENERATE_WORKUNITS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "10");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(10));
  }

  @Test
  public void testRecommendScalingActivityConfigurationStrategy() {
    ActivityConfigurationStrategy.RecommendScalingActivityConfigurationStrategy strategy =
        new ActivityConfigurationStrategy.RecommendScalingActivityConfigurationStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityConfigurationStrategy.DEFAULT_RECOMMEND_SCALING_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.RECOMMEND_SCALING_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "13");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(13));
  }

  @Test
  public void testDeleteWorkDirsActivityConfigurationStrategy() {
    ActivityConfigurationStrategy.DeleteWorkDirsActivityConfigurationStrategy strategy =
        new ActivityConfigurationStrategy.DeleteWorkDirsActivityConfigurationStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityConfigurationStrategy.DEFAULT_DELETE_WORK_DIRS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.DELETE_WORK_DIRS_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "27");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(27));
  }

  @Test
  public void testProcessWorkunitActivityConfigurationStrategy() {
    ActivityConfigurationStrategy.ProcessWorkunitActivityConfigurationStrategy strategy =
        new ActivityConfigurationStrategy.ProcessWorkunitActivityConfigurationStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityConfigurationStrategy.DEFAULT_PROCESS_WORKUNIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.PROCESS_WORKUNIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "33");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(33));
  }

  @Test
  public void testCommitActivityConfigurationStrategy() {
    ActivityConfigurationStrategy.CommitActivityConfigurationStrategy strategy =
        new ActivityConfigurationStrategy.CommitActivityConfigurationStrategy();
    // Test default timeout
    Assert.assertEquals(strategy.getStartToCloseTimeout(new Properties()),
        Duration.ofMinutes(ActivityConfigurationStrategy.DEFAULT_COMMIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES));
    // Test custom timeout
    Properties props = new Properties();
    props.setProperty(GobblinTemporalConfigurationKeys.COMMIT_ACTIVITY_STARTTOCLOSE_TIMEOUT_MINUTES, "33");
    Assert.assertEquals(strategy.getStartToCloseTimeout(props), Duration.ofMinutes(33));
  }
}
