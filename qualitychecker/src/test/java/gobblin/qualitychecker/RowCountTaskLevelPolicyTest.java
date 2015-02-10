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
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import gobblin.qualitychecker.task.TaskLevelPolicyChecker;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckerBuilderFactory;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.State;
import gobblin.qualitychecker.task.TaskLevelPolicy;


@Test(groups = {"gobblin.qualitychecker"})
public class RowCountTaskLevelPolicyTest {

  public static final long EXTRACTOR_ROWS_READ = 1000;
  public static final long WRITER_ROWS_WRITTEN = 1000;

  @Test
  public void testRowCountPolicyPassed()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "gobblin.policies.count.RowCountPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, EXTRACTOR_ROWS_READ);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, WRITER_ROWS_WRITTEN);

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
    }
  }

  @Test
  public void testRowCountPolicyFailed()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "gobblin.policies.count.RowCountPolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, EXTRACTOR_ROWS_READ);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, -1);

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.FAILED);
    }
  }

  @Test
  public void testRowCountRangePolicyPassedExact()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "gobblin.policies.count.RowCountRangePolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, EXTRACTOR_ROWS_READ);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, WRITER_ROWS_WRITTEN);
    state.setProp(ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
    }
  }

  @Test
  public void testRowCountRangePolicyPassedRange()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "gobblin.policies.count.RowCountRangePolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, EXTRACTOR_ROWS_READ);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, (long) 0.03 * EXTRACTOR_ROWS_READ + EXTRACTOR_ROWS_READ);
    state.setProp(ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.PASSED);
    }
  }

  @Test
  public void testRowCountRangePolicyFailed()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST, "gobblin.policies.count.RowCountRangePolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL");
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, EXTRACTOR_ROWS_READ);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, -1);
    state.setProp(ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

    TaskLevelPolicyCheckResults results = getPolicyResults(state);
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      Assert.assertEquals(entry.getKey(), TaskLevelPolicy.Result.FAILED);
    }
  }

  @Test
  public void testMultipleRowCountPolicies()
      throws Exception {
    State state = new State();
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST,
        "gobblin.policies.count.RowCountPolicy,gobblin.policies.count.RowCountRangePolicy");
    state.setProp(ConfigurationKeys.TASK_LEVEL_POLICY_LIST_TYPE, "FAIL,FAIL");
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, EXTRACTOR_ROWS_READ);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, WRITER_ROWS_WRITTEN);
    state.setProp(ConfigurationKeys.ROW_COUNT_RANGE, "0.05");

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
