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

package gobblin.fork;

import gobblin.configuration.ConfigurationKeys;
import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;


/**
 * An implementation of {@link ForkOperator} that simply copy the input schema
 * and data record into each forked branch. This class is useful if a converted
 * data record needs to be written to different destinations.
 *
 * @author ynli
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

    schemas.clear();
    for (int i = 0; i < getBranches(workUnitState); i++) {
      schemas.add(Boolean.TRUE);
    }

    return schemas;
  }

  @Override
  public List<Boolean> forkDataRecord(WorkUnitState workUnitState, D input) {

    records.clear();
    for (int i = 0; i < getBranches(workUnitState); i++) {
      records.add(Boolean.TRUE);
    }

    return records;
  }

  @Override
  public void close()
      throws IOException {
    // Nothing to do
  }
}
