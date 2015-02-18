/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.qualitychecker;

import gobblin.configuration.ConfigurationKeys;
import gobblin.qualitychecker.task.TaskLevelPolicy;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckerBuilderFactory;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.State;


@Test(groups = {"gobblin.qualitychecker"})
public class TaskLevelQualityCheckerTest {

  @Test
  public void testPolicyChecker()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "gobblin.qualitychecker.TestTaskLevelPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
    }
  }

  @Test
  public void testMultiplePolicies()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST,
        "gobblin.qualitychecker.TestTaskLevelPolicy,gobblin.qualitychecker.TestTaskLevelPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL,FAIL");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
    }
  }

  private TaskLevelPolicyCheckResults getPolicyResults(State state)
      throws Exception {
    TaskLevelPolicyChecker checker =
        new TaskLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    return checker.executePolicies();
  }
}
