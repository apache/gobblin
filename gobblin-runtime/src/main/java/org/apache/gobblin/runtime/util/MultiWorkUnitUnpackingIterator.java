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

package org.apache.gobblin.runtime.util;

import java.util.Iterator;

import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;

import lombok.RequiredArgsConstructor;


/**
 * An {@link Iterator} that unpacks {@link MultiWorkUnit}s.
 */
@RequiredArgsConstructor
public class MultiWorkUnitUnpackingIterator implements Iterator<WorkUnit> {
  private final Iterator<WorkUnit> workUnits;

  /** The iterator for {@link #nextWu} if it's a {@link MultiWorkUnit} */
  private Iterator<WorkUnit> currentIterator;
  /** The work unit to be checked in {@link #next()} */
  private WorkUnit nextWu;
  /** A flag indicating if a new seek operation will be needed */
  private boolean needSeek = true;

  @Override
  public boolean hasNext() {
    seekNext();
    return nextWu != null;
  }

  @Override
  public WorkUnit next() {
    // In case, the caller forgets to call hasNext()
    seekNext();

    WorkUnit wu = nextWu;
    if (nextWu instanceof MultiWorkUnit) {
      wu = this.currentIterator.next();
    }
    needSeek = true;
    return wu;
  }

  /** Seek to the next available work unit, skipping all empty work units */
  private void seekNext() {
    if (!needSeek) {
      return;
    }

    // First, iterate all
    if (this.currentIterator != null && this.currentIterator.hasNext()) {
      needSeek = false;
      return;
    }

    // Then, find the next available work unit
    nextWu = null;
    this.currentIterator = null;
    while (nextWu == null && workUnits.hasNext()) {
      nextWu = workUnits.next();
      if (nextWu instanceof MultiWorkUnit) {
        this.currentIterator = ((MultiWorkUnit) nextWu).getWorkUnits().iterator();
        if (!this.currentIterator.hasNext()) {
          nextWu = null;
        }
      }
    }

    needSeek = false;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
