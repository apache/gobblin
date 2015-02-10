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

import java.util.HashMap;
import java.util.Map;


/**
 * Wrapper around a Map of PolicyResults and Policy.Type
 */
public class TaskLevelPolicyCheckResults {
  private final Map<TaskLevelPolicy.Result, TaskLevelPolicy.Type> results;

  public TaskLevelPolicyCheckResults() {
    this.results = new HashMap<TaskLevelPolicy.Result, TaskLevelPolicy.Type>();
  }

  public Map<TaskLevelPolicy.Result, TaskLevelPolicy.Type> getPolicyResults() {
    return this.results;
  }
}