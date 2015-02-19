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

package gobblin.qualitychecker.row;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;

/**
 * A policy that operates on each row
 * and executes a given check
 * @author stakiar
 */
public abstract class RowLevelPolicy {
  private final State state;
  private final Type type;

  public enum Type {
    FAIL,          // Fail if the test does not pass
    ERR_FILE,      // Write record to error file
    OPTIONAL       // The test is optional
  }

  ;

  public enum Result {
    PASSED,          // The test passed
    FAILED           // The test failed
  }

  ;

  public RowLevelPolicy(State state, RowLevelPolicy.Type type) {
    this.state = state;
    this.type = type;
  }

  public State getTaskState() {
    return state;
  }

  public Type getType() {
    return type;
  }

  public String getErrFileLocation() {
    return this.state.getProp(ConfigurationKeys.ROW_LEVEL_ERR_FILE);
  }

  public abstract Result executePolicy(Object record);

  @Override
  public String toString() {
    return this.getClass().getName();
  }
}
