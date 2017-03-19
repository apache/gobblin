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

package gobblin.runtime.util;

import java.util.Iterator;

import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;

import lombok.RequiredArgsConstructor;


/**
 * An {@link Iterator} that unpacks {@link MultiWorkUnit}s.
 */
@RequiredArgsConstructor
public class MultiWorkUnitUnpackingIterator implements Iterator<WorkUnit> {
  private final Iterator<WorkUnit> workUnits;
  private Iterator<WorkUnit> currentIterator;

  @Override
  public boolean hasNext() {
    return this.workUnits.hasNext() || (this.currentIterator != null && this.currentIterator.hasNext());
  }

  @Override
  public WorkUnit next() {
    if (this.currentIterator != null && this.currentIterator.hasNext()) {
      WorkUnit next = this.currentIterator.next();
      if (next instanceof MultiWorkUnit) {
        throw new IllegalStateException("A MultiWorkUnit cannot contain other MultiWorkUnits.");
      }
      return next;
    }
    WorkUnit wu = this.workUnits.next();
    if (wu instanceof MultiWorkUnit) {
      this.currentIterator = ((MultiWorkUnit) wu).getWorkUnits().iterator();
      return next();
    } else {
      return wu;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
