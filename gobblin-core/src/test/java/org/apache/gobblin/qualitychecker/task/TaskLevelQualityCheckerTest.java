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

package org.apache.gobblin.qualitychecker.task;

import java.util.Map;
import java.util.Set;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.qualitychecker"})
public class TaskLevelQualityCheckerTest {

  @Test
  public void testPolicyChecker()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "org.apache.gobblin.qualitychecker.TestTaskLevelPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, Set<TaskLevelPolicy.Type>> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
    }
  }

  @Test
  public void testMultiplePoliciesAllPass()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST,
        "org.apache.gobblin.qualitychecker.TestTaskLevelPolicy,org.apache.gobblin.qualitychecker.TestTaskLevelPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL,FAIL");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, Set<TaskLevelPolicy.Type>> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
    }
  }

  @Test
  public void testMultiplePoliciesAllFail()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST,
        "org.apache.gobblin.qualitychecker.FailingTaskLevelPolicy,org.apache.gobblin.qualitychecker.FailingTaskLevelPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL,FAIL");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, Set<TaskLevelPolicy.Type>> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.FAILED);
    }
  }

  @Test
  public void testMultiplePoliciesAllFailWithDifferentPolicyTypes()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST,
        "org.apache.gobblin.qualitychecker.FailingTaskLevelPolicy,org.apache.gobblin.qualitychecker.FailingTaskLevelPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "OPTIONAL,FAIL");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    Assert.assertEquals(results.getPolicyResults().get(TaskLevelPolicy.Result.FAILED).size(), 2);
  }

  @Test
  public void testMultiplePoliciesFailAndPassWithDifferentPolicyTypes()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST,
        "org.apache.gobblin.qualitychecker.FailingTaskLevelPolicy,org.apache.gobblin.qualitychecker.FailingTaskLevelPolicy,org.apache.gobblin.qualitychecker.TestTaskLevelPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "OPTIONAL,FAIL,FAIL");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    Assert.assertEquals(results.getPolicyResults().get(TaskLevelPolicy.Result.FAILED).size(), 2);
    Assert.assertEquals(results.getPolicyResults().get(TaskLevelPolicy.Result.PASSED).size(), 1);
    Assert.assertTrue(results.getPolicyResults().get(TaskLevelPolicy.Result.PASSED).contains(TaskLevelPolicy.Type.FAIL));
    Assert.assertTrue(results.getPolicyResults().get(TaskLevelPolicy.Result.FAILED).contains(TaskLevelPolicy.Type.FAIL));
    Assert.assertTrue(results.getPolicyResults().get(TaskLevelPolicy.Result.FAILED).contains(TaskLevelPolicy.Type.OPTIONAL));
  }

  private TaskLevelPolicyCheckResults getPolicyResults(State state)
      throws Exception {
    TaskLevelPolicyChecker checker =
        new TaskLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(state, -1).build();
    return checker.executePolicies();
  }
}
