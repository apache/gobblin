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

import gobblin.configuration.State;
import gobblin.qualitychecker.task.TaskLevelPolicy;


public class TestTaskLevelPolicy extends TaskLevelPolicy {
  public TestTaskLevelPolicy(State state, Type type) {
    super(state, type);
  }

  @Override
  public Result executePolicy() {
    return Result.PASSED;
  }
}