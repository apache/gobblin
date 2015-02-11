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

package gobblin.publisher;

import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.qualitychecker.task.TaskLevelPolicy;


public class TaskPublisher {
  private final TaskLevelPolicyCheckResults results;
  private final WorkUnitState workUnitState;

  private static final Logger LOG = LoggerFactory.getLogger(TaskPublisher.class);

  public enum PublisherState {
    SUCCESS,                 // Data and metadata are successfully published
    CLEANUP_FAIL,            // Data and metadata were published, but cleanup failed
    POLICY_TESTS_FAIL,       // All tests didn't pass, no data committed
    COMPONENTS_NOT_FINISHED  // All components did not complete, no data committed
  }

  ;

  public TaskPublisher(WorkUnitState workUnitState, TaskLevelPolicyCheckResults results)
      throws Exception {

    this.results = results;
    this.workUnitState = workUnitState;
  }

  public PublisherState canPublish()
      throws Exception {
    if (allComponentsFinished()) {
      LOG.info("All components finished successfully, checking quality tests");
      if (passedAllTests()) {
        LOG.info("All required test passed for this task passed.");
        if (cleanup()) {
          LOG.info("Cleanup for task publisher executed successfully.");
          return PublisherState.SUCCESS;
        } else {
          return PublisherState.CLEANUP_FAIL;
        }
      } else {
        return PublisherState.POLICY_TESTS_FAIL;
      }
    } else {
      return PublisherState.COMPONENTS_NOT_FINISHED;
    }
  }

  /**
   * Returns true if all tests from the PolicyChecker pass, false otherwise
   */
  public boolean passedAllTests() {
    for (Map.Entry<TaskLevelPolicy.Result, TaskLevelPolicy.Type> entry : results.getPolicyResults().entrySet()) {
      if (entry.getKey().equals(TaskLevelPolicy.Result.FAILED) && entry.getValue().equals(TaskLevelPolicy.Type.FAIL)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if all the components finished, false otherwise
   */
  public boolean allComponentsFinished() {
    // Have to parse some information from TaskState
    return true;
  }

  /**
   * Cleans up any tmp folders used by the Task
   * Return true if successful, false otherwise
   */
  public boolean cleanup()
      throws Exception {
    return true;
  }
}
