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
import lombok.Setter;


/**
 * A stream of {@link WorkUnit}s, allows for working with large numbers of work units in a memory-efficient way, as
 * well as processing infinite streams of work units.
 */
public interface WorkUnitStream {

  /**
   * @return Iterator of {@link WorkUnit}s.
   */
  Iterator<WorkUnit> getWorkUnits();

  /**
   * @return true if this {@link WorkUnitStream} is finite.
   */
  boolean isFiniteStream();

  /**
   * Apply a transformation function to this stream.
   */
  WorkUnitStream transform(Function<WorkUnit, WorkUnit> function);

  /**
   * Apply a filtering function to this stream.
   */
  WorkUnitStream filter(Predicate<WorkUnit> predicate);

  /**
   * @return true if it is safe to call {@link #getMaterializedWorkUnitCollection()} to get a collection of
   *          {@link WorkUnit}s.
   */
  boolean isSafeToMaterialize();

  /**
   * Get a materialized collection of the {@link WorkUnit}s in this stream. Note this call will fail if
   * {@link #isSafeToMaterialize()} is false. This method should be avoided unless strictly necessary, as it
   * introduces a synchronization point for all work units, removing the speed and memory-efficiency benefits.
   */
  Collection<WorkUnit> getMaterializedWorkUnitCollection();
}
