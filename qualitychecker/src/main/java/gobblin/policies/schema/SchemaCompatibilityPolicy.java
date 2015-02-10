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

package gobblin.policies.schema;

import gobblin.configuration.ConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.configuration.State;
import gobblin.qualitychecker.task.TaskLevelPolicy;


public class SchemaCompatibilityPolicy extends TaskLevelPolicy {
  private static final Logger log = LoggerFactory.getLogger(SchemaCompatibilityPolicy.class);

  private State state;
  private State previousState;

  public SchemaCompatibilityPolicy(State state, Type type) {
    super(state, type);
    this.state = state;
    this.previousState = this.getPreviousTableState();
  }

  @Override
  public Result executePolicy() {
    // TODO how do you test for backwards compatibility?
    if (previousState.getProp(ConfigurationKeys.EXTRACT_SCHEMA) == null) {
      log.info("Previous Task State does not contain a schema");
      return Result.PASSED;
    }

    if (state.getProp(ConfigurationKeys.EXTRACT_SCHEMA)
        .equals(previousState.getProp(ConfigurationKeys.EXTRACT_SCHEMA))) {
      return Result.PASSED;
    } else {
      return Result.FAILED;
    }
  }
}