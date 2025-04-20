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

import java.util.List;

import org.apache.gobblin.configuration.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PolicyChecker takes in a list of Policy objects
 * executes each one, and then stores the output
 * in a PolicyCheckResults object
 */
public class TaskLevelPolicyChecker {
  /**
   * An enumeration for possible statuses for Data quality checks,
   * its values will be PASSED, FAILED, in case if data quality check
   * evaluation is not performed for Job, it will be NOT_EVALUATED
   */
  public enum DataQualityStatus {
    PASSED,
    FAILED,
    NOT_EVALUATED
  }
  private final List<TaskLevelPolicy> list;
  private static final Logger LOG = LoggerFactory.getLogger(TaskLevelPolicyChecker.class);

  public static final String TASK_LEVEL_POLICY_RESULT_KEY = "gobblin.task.level.policy.result";

  public TaskLevelPolicyChecker(List<TaskLevelPolicy> list) {
    this.list = list;
  }

  public TaskLevelPolicyCheckResults executePolicies() {
    TaskLevelPolicyCheckResults results = new TaskLevelPolicyCheckResults();
    boolean allRequiredPoliciesPassed = true;
    State state = this.list.get(0).getTaskState();
    for (TaskLevelPolicy p : this.list) {
      TaskLevelPolicy.Result result = p.executePolicy();
      results.getPolicyResults().put(result, p.getType());
      if(TaskLevelPolicy.Type.FAIL.equals(p.getType()) && TaskLevelPolicy.Result.FAILED.name().equals(result.name())){
        allRequiredPoliciesPassed = false;
      }
      LOG.info("TaskLevelPolicy " + p + " of type " + p.getType() + " executed with result " + result);
    }
    state.setProp(TASK_LEVEL_POLICY_RESULT_KEY, allRequiredPoliciesPassed ? "PASSED" : "FAILED");
    return results;
  }
}
