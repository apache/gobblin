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

package org.apache.gobblin.temporal.util.nesting.work;

import java.util.NoSuchElementException;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import com.fasterxml.jackson.annotation.JsonIgnore;


/** Logical sub-sequence of `WORK_ITEM`s, backed for simplicity's sake by an in-memory collection, *SHARED* w/ other work spans */
@NoArgsConstructor
@RequiredArgsConstructor
public class SeqSliceBackedWorkSpan<WORK_ITEM> implements Workload.WorkSpan<WORK_ITEM> {
  private static final int NOT_SET_SENTINEL = -1;

  @NonNull private WORK_ITEM[] sharedElems;
  // CAUTION: despite the "warning: @NonNull is meaningless on a primitive @lombok.RequiredArgsConstructor"...
  // if removed, no two-arg ctor is generated, so syntax error on `new CollectionSliceBackedTaskSpan(elems, startIndex)`
  @NonNull private int startingIndex;
  @NonNull private int numElements;
  private transient volatile int nextElemIndex = NOT_SET_SENTINEL;

  @Override
  public int getNumElems() {
    return getEndingIndex() - startingIndex;
  }

  @Override
  public boolean hasNext() {
    if (nextElemIndex == NOT_SET_SENTINEL) {
      nextElemIndex = startingIndex; // NOTE: `startingIndex` should be effectively `final` (post-deser) and always >= 0
    }
    return nextElemIndex < this.getEndingIndex();
  }

  @Override
  public WORK_ITEM next() {
    if (nextElemIndex >= startingIndex + numElements) {
      throw new NoSuchElementException("index " + nextElemIndex + " >= " + startingIndex + " + " + numElements);
    }
    return sharedElems[nextElemIndex++];
  }

  @Override
  public String toString() {
    return getClassNickname() + "(" + startingIndex + "... {+" + getNumElems() + "})";
  }

  protected String getClassNickname() {
    // return getClass().getSimpleName();
    return "WorkSpan";
  }

  @JsonIgnore // (because no-arg method resembles 'java bean property')
  protected final int getEndingIndex() {
    return Math.min(startingIndex + numElements, sharedElems.length);
  }
}
