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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.TestTaskLevelPolicy;

@Test
public class TaskLevelPolicyCheckerTest {

  @Test
  public void testSinglePolicyPassed() {
    // Create a state with a single policy that always passes
    State state = new State();
    List<TaskLevelPolicy> policies = new ArrayList<>();
    policies.add(new TestTaskLevelPolicy(state, TaskLevelPolicy.Type.FAIL));

    // Create checker and execute policies
    TaskLevelPolicyChecker checker = new TaskLevelPolicyChecker(policies);
    TaskLevelPolicyCheckResults results = checker.executePolicies();

    // Verify results
    Assert.assertEquals(results.getPolicyResults().size(), 1);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
      Assert.assertEquals(entry.getValue(), TaskLevelPolicy.Type.FAIL);
    }
    Assert.assertEquals(state.getProp(TaskLevelPolicyChecker.TASK_LEVEL_POLICY_RESULT_KEY),
        TaskLevelPolicyChecker.DataQualityStatus.PASSED.name());
  }

  @Test
  public void testSinglePolicyFailed() {
    // Create a state with a single policy that always fails
    State state = new State();
    List<TaskLevelPolicy> policies = new ArrayList<>();
    policies.add(new FailingTaskLevelPolicy(state, TaskLevelPolicy.Type.FAIL));

    // Create checker and execute policies
    TaskLevelPolicyChecker checker = new TaskLevelPolicyChecker(policies);
    TaskLevelPolicyCheckResults results = checker.executePolicies();

    // Verify results
    Assert.assertEquals(results.getPolicyResults().size(), 1);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.FAILED);
      Assert.assertEquals(entry.getValue(), TaskLevelPolicy.Type.FAIL);
    }
    Assert.assertEquals(state.getProp(TaskLevelPolicyChecker.TASK_LEVEL_POLICY_RESULT_KEY), TaskLevelPolicyChecker.DataQualityStatus.FAILED.name());
  }

  @Test
  public void testMultiplePoliciesMixedResults() {
    // Create a state with multiple policies having mixed results
    State state = new State();
    List<TaskLevelPolicy> policies = new ArrayList<>();
    policies.add(new TestTaskLevelPolicy(state, TaskLevelPolicy.Type.FAIL)); // Passes
    policies.add(new FailingTaskLevelPolicy(state, TaskLevelPolicy.Type.FAIL)); // Fails
    policies.add(new TestTaskLevelPolicy(state, TaskLevelPolicy.Type.OPTIONAL)); // Passes

    // Create checker and execute policies
    TaskLevelPolicyChecker checker = new TaskLevelPolicyChecker(policies);
    TaskLevelPolicyCheckResults results = checker.executePolicies();

    // Verify results
    Assert.assertEquals(results.getPolicyResults().size(), 2);
    int passedCount = 0;
    int failedCount = 0;
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      if (entry.getKey() == TaskLevelPolicy.Result.PASSED) {
        passedCount++;
      } else {
        failedCount++;
      }
    }
    Assert.assertEquals(passedCount, 1);
    Assert.assertEquals(failedCount, 1);
    Assert.assertEquals(state.getProp(TaskLevelPolicyChecker.TASK_LEVEL_POLICY_RESULT_KEY),
        TaskLevelPolicyChecker.DataQualityStatus.FAILED.name());
  }

  @Test
  public void testOptionalPolicyFailure() {
    // Create a state with an optional policy that fails
    State state = new State();
    List<TaskLevelPolicy> policies = new ArrayList<>();
    policies.add(new FailingTaskLevelPolicy(state, TaskLevelPolicy.Type.OPTIONAL));

    // Create checker and execute policies
    TaskLevelPolicyChecker checker = new TaskLevelPolicyChecker(policies);
    TaskLevelPolicyCheckResults results = checker.executePolicies();

    // Verify results
    Assert.assertEquals(results.getPolicyResults().size(), 1);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.FAILED);
      Assert.assertEquals(entry.getValue(), TaskLevelPolicy.Type.OPTIONAL);
    }
    Assert.assertEquals(state.getProp(TaskLevelPolicyChecker.TASK_LEVEL_POLICY_RESULT_KEY), TaskLevelPolicyChecker.DataQualityStatus.PASSED.name());
  }

  // Helper class for testing failing policies
  private static class FailingTaskLevelPolicy extends TaskLevelPolicy {
    public FailingTaskLevelPolicy(State state, Type type) {
      super(state, type);
    }

    @Override
    public Result executePolicy() {
      return Result.FAILED;
    }
  }
}