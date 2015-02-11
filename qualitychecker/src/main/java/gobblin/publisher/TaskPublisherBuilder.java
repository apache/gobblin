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

import gobblin.configuration.WorkUnitState;
import gobblin.qualitychecker.task.TaskLevelPolicyCheckResults;


public class TaskPublisherBuilder {
  private final TaskLevelPolicyCheckResults results;
  private final WorkUnitState workUnitState;
  private final int index;

  public TaskPublisherBuilder(WorkUnitState workUnitState, TaskLevelPolicyCheckResults results, int index) {
    this.results = results;
    this.workUnitState = workUnitState;
    this.index = index;
  }

  public static TaskPublisherBuilder newBuilder(WorkUnitState taskState, TaskLevelPolicyCheckResults results,
      int index) {

    return new TaskPublisherBuilder(taskState, results, index);
  }

  public TaskPublisher build()
      throws Exception {
    return new TaskPublisher(this.workUnitState, this.results);
  }
}
