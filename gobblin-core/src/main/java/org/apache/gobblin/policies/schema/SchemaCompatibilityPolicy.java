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

package org.apache.gobblin.policies.schema;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.qualitychecker.task.TaskLevelPolicy;


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
    if (this.previousState.getProp(ConfigurationKeys.EXTRACT_SCHEMA) == null) {
      log.info("Previous Task State does not contain a schema");
      return Result.PASSED;
    }

    if (this.state.getProp(ConfigurationKeys.EXTRACT_SCHEMA)
        .equals(this.previousState.getProp(ConfigurationKeys.EXTRACT_SCHEMA))) {
      return Result.PASSED;
    }
    return Result.FAILED;
  }
}
