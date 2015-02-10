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

package gobblin.source.workunit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;


/**
 * A class that wraps multiple {@link WorkUnit}s so they can executed within a single task.
 *
 * @author ynli
 */
public class MultiWorkUnit extends WorkUnit {

  private final List<WorkUnit> workUnits = Lists.newArrayList();

  /**
   * Get an immutable list of {@link WorkUnit}s wrapped by this {@link MultiWorkUnit}.
   *
   * @return immutable list of {@link WorkUnit}s wrapped by this {@link MultiWorkUnit}
   */
  public List<WorkUnit> getWorkUnits() {
    return ImmutableList.<WorkUnit>builder().addAll(this.workUnits).build();
  }

  /**
   * Add a single {@link WorkUnit}.
   *
   * @param workUnit {@link WorkUnit} to add
   */
  public void addWorkUnit(WorkUnit workUnit) {
    this.workUnits.add(workUnit);
  }

  /**
   * Add a collection of {@link WorkUnit}s.
   *
   * @param workUnits collection of {@link WorkUnit}s to add
   */
  public void addWorkUnits(Collection<WorkUnit> workUnits) {
    this.workUnits.addAll(workUnits);
  }

  @Override
  public void readFields(DataInput in)
      throws IOException {
    int numWorkUnits = in.readInt();
    for (int i = 0; i < numWorkUnits; i++) {
      WorkUnit workUnit = new WorkUnit(null, null);
      workUnit.readFields(in);
      this.workUnits.add(workUnit);
    }
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    out.writeInt(this.workUnits.size());
    for (WorkUnit workUnit : this.workUnits) {
      workUnit.write(out);
    }
  }
}
