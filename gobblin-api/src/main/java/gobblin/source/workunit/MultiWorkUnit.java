/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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
 * <p>
 *  This class also extends the {@link gobblin.configuration.State} object and thus it is possible to set and get
 *  properties from this class. The {@link #setProp(String, Object)} method will add the specified key, value pair to
 *  this class as well as to every {@link WorkUnit} in {@link #workUnits}. The {@link #getProp(String)} methods will
 *  only return properties that have been explicitily set in this class (e.g. it will not retrieve properties from
 *  {@link #workUnits}.
 * </p>
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

  /**
   * Set the specified key, value pair in this {@link MultiWorkUnit} as well as in all the inner {@link WorkUnit}s.
   *
   * {@inheritDoc}
   * @see gobblin.configuration.State#setProp(java.lang.String, java.lang.Object)
   */
  @Override
  public void setProp(String key, Object value) {
    super.setProp(key, value);
    for (WorkUnit workUnit : this.workUnits) {
      workUnit.setProp(key, value);
    }
  }

  /**
   * Set the specified key, value pair in this {@link MultiWorkUnit} only, but do not propagate it to all the inner
   * {@link WorkUnit}s.
   *
   * @param key property key
   * @param value property value
   */
  public void setPropExcludeInnerWorkUnits(String key, Object value) {
    super.setProp(key, value);
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
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out)
      throws IOException {
    out.writeInt(this.workUnits.size());
    for (WorkUnit workUnit : this.workUnits) {
      workUnit.write(out);
    }
    super.write(out);
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof MultiWorkUnit)) {
      return false;
    }

    MultiWorkUnit other = (MultiWorkUnit) object;
    return super.equals(other) && this.workUnits.equals(other.workUnits);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((this.workUnits == null) ? 0 : this.workUnits.hashCode());
    return result;
  }
}
