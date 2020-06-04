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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SharedResourcesBrokerImpl;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.source.workunit.WorkUnit;

import static org.apache.gobblin.util.retry.RetryerFactory.RETRY_TIME_OUT_MS;


public class GobblinMultiTaskAttemptTest {
  private GobblinMultiTaskAttempt taskAttempt;

  @Test
  public void testRunWithTaskCreationFailure()
      throws Exception {
    // Preparing Instance of TaskAttempt with designed failure on task creation
    WorkUnit tmpWU = new WorkUnit();
    // Put necessary attributes in workunit
    tmpWU.setProp(ConfigurationKeys.TASK_ID_KEY, "task_test");
    List<WorkUnit> workUnit = ImmutableList.of(tmpWU);
    JobState jobState = new JobState();
    // Limit the number of times of retry in task-creation.
    jobState.setProp(RETRY_TIME_OUT_MS, 1000);
    TaskStateTracker stateTrackerMock = Mockito.mock(TaskStateTracker.class);
    TaskExecutor taskExecutorMock = Mockito.mock(TaskExecutor.class);
    Config config = ConfigFactory.empty();
    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBrokerImpl<GobblinScopeTypes> jobBroker =
        topBroker.newSubscopedBuilder(new JobScopeInstance("testJob", "job123")).build();
    taskAttempt =
        new GobblinMultiTaskAttempt(workUnit.iterator(), "testJob", jobState, stateTrackerMock, taskExecutorMock,
            Optional.absent(), Optional.absent(), jobBroker);

    try {
      // This attempt will automatically fail due to missing required config in
      // org.apache.gobblin.runtime.TaskContext.getSource
      taskAttempt.run();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
      return;
    }

    // Should never reach here.
    Assert.fail();
  }
}