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

package gobblin.fork;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;


/**
 * An implementation of {@link ForkOperator} that simply copy the input schema
 * and data record into each forked branch. This class is useful if a converted
 * data record needs to be written to different destinations.
 *
 * @author Yinan Li
 */
public class IdentityForkOperator<S, D> implements ForkOperator<S, D> {

  // Reuse both lists to save the cost of allocating new lists
  private final List<Boolean> schemas = Lists.newArrayList();
  private final List<Boolean> records = Lists.newArrayList();

  @Override
  public void init(WorkUnitState workUnitState) {
    // Do nothing
  }

  @Override
  public int getBranches(WorkUnitState workUnitState) {
    return workUnitState.getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
  }

  @Override
  public List<Boolean> forkSchema(WorkUnitState workUnitState, S input) {
    this.schemas.clear();
    for (int i = 0; i < getBranches(workUnitState); i++) {
      this.schemas.add(Boolean.TRUE);
    }

    return this.schemas;
  }

  @Override
  public List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input) {
    this.records.clear();
    for (int i = 0; i < getBranches(workUnitState); i++) {
      this.records.add(Boolean.TRUE);
    }

    return this.records;
  }

  @Override
  public void close()
      throws IOException {
    // Nothing to do
  }
}
