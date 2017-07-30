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

package gobblin.qualitychecker.task;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


public abstract class TaskLevelPolicy {
  private final State state;
  private final Type type;

  public enum Type {
    FAIL, // Fail if the test does not pass
    OPTIONAL // The test is optional
  }

  public enum Result {
    PASSED, // The test passed
    FAILED // The test failed
  }

  public TaskLevelPolicy(State state, TaskLevelPolicy.Type type) {
    this.state = state;
    this.type = type;
  }

  public State getTaskState() {
    return this.state;
  }

  public Type getType() {
    return this.type;
  }

  public abstract Result executePolicy();

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  public State getPreviousTableState() {
    WorkUnitState workUnitState = (WorkUnitState) this.state;
    return workUnitState.getPreviousTableState();
  }
}
