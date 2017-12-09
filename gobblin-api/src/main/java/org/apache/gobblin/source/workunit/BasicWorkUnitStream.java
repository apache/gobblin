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

package org.apache.gobblin.source.workunit;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import lombok.Getter;


/**
 * A basic implementation of {@link WorkUnitStream}.
 */
public class BasicWorkUnitStream implements WorkUnitStream {

  /**
   * Iterator over the {@link WorkUnit}s. Best practice is to only generate {@link WorkUnit}s on demand to save memory.
   */
  private final Iterator<WorkUnit> workUnits;

  private List<WorkUnit> materializedWorkUnits;
  /**
   * If true, this stream can be safely consumed before processing any work units.
   * Note not all job launchers support infinite streams.
   */
  @Getter
  private final boolean finiteStream;
  /**
   * If true, this {@link WorkUnitStream} can be retrieved as a collection of {@link WorkUnit}s.
   */
  @Getter
  private final boolean safeToMaterialize;

  BasicWorkUnitStream(Iterator<WorkUnit> workUnits, List<WorkUnit> materializedWorkUnits, boolean finiteStream, boolean safeToMaterialize) {
    this.workUnits = workUnits;
    this.materializedWorkUnits = materializedWorkUnits;
    this.finiteStream = finiteStream;
    this.safeToMaterialize = safeToMaterialize;
  }

  private BasicWorkUnitStream(BasicWorkUnitStream other, Iterator<WorkUnit> workUnits, List<WorkUnit> materializedWorkUnits) {
    this.workUnits = workUnits;
    this.materializedWorkUnits = materializedWorkUnits;
    this.finiteStream = other.finiteStream;
    this.safeToMaterialize = other.safeToMaterialize;
  }

  public Iterator<WorkUnit> getWorkUnits() {
    if (this.materializedWorkUnits == null) {
      return this.workUnits;
    } else {
      return this.materializedWorkUnits.iterator();
    }
  }

  /**
   * Apply a transformation function to this stream.
   */
  public WorkUnitStream transform(Function<WorkUnit, WorkUnit> function) {
    if (this.materializedWorkUnits == null) {
      return new BasicWorkUnitStream(this, Iterators.transform(this.workUnits, function), null);
    } else {
      return new BasicWorkUnitStream(this, null, Lists.newArrayList(Lists.transform(this.materializedWorkUnits, function)));
    }
  }

  /**
   * Apply a filtering function to this stream.
   */
  public WorkUnitStream filter(Predicate<WorkUnit> predicate) {
    if (this.materializedWorkUnits == null) {
      return new BasicWorkUnitStream(this, Iterators.filter(this.workUnits, predicate), null);
    } else {
      return new BasicWorkUnitStream(this, null, Lists.newArrayList(Iterables.filter(this.materializedWorkUnits, predicate)));
    }
  }

  /**
   * Get a materialized collection of the {@link WorkUnit}s in this stream. Note this call will fail if
   * {@link #isSafeToMaterialize()} is false.
   */
  public Collection<WorkUnit> getMaterializedWorkUnitCollection() {
    materialize();
    return this.materializedWorkUnits;
  }

  private void materialize() {
    if (this.materializedWorkUnits != null) {
      return;
    }
    if (!isSafeToMaterialize()) {
      throw new UnsupportedOperationException("WorkUnitStream is not safe to materialize.");
    }
    this.materializedWorkUnits = Lists.newArrayList(this.workUnits);
  }

  public static class Builder {
    private Iterator<WorkUnit> workUnits;
    private List<WorkUnit> workUnitList;
    private boolean finiteStream = true;
    private boolean safeToMaterialize = false;


    public Builder(Iterator<WorkUnit> workUnits) {
      this.workUnits = workUnits;
    }

    public Builder(List<WorkUnit> workUnits) {
      this.workUnitList = workUnits;
      this.safeToMaterialize = true;
      this.finiteStream = true;
    }

    public Builder setFiniteStream(boolean finiteStream) {
      this.finiteStream = finiteStream;
      return this;
    }

    public Builder setSafeToMaterialize(boolean safeToMaterialize) {
      this.safeToMaterialize = safeToMaterialize;
      return this;
    }

    public WorkUnitStream build() {
      return new BasicWorkUnitStream(this.workUnits, this.workUnitList, this.finiteStream, this.safeToMaterialize);
    }
  }

}
