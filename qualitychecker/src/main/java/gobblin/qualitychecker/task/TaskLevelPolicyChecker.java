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

package gobblin.qualitychecker.task;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PolicyChecker takes in a list of Policy objects
 * executes each one, and then stores the output
 * in a PolicyCheckResults object
 */
public class TaskLevelPolicyChecker {
  private final List<TaskLevelPolicy> list;
  private static final Logger LOG = LoggerFactory.getLogger(TaskLevelPolicyChecker.class);

  public TaskLevelPolicyChecker(List<TaskLevelPolicy> list) {
    this.list = list;
  }

  public TaskLevelPolicyCheckResults executePolicies() {
    TaskLevelPolicyCheckResults results = new TaskLevelPolicyCheckResults();
    for (TaskLevelPolicy p : this.list) {
      TaskLevelPolicy.Result result = p.executePolicy();
      results.getPolicyResults().put(result, p.getType());
      LOG.info("TaskLevelPolicy " + p + " of type " + p.getType() + " executed with result " + result);
    }
    return results;
  }
}