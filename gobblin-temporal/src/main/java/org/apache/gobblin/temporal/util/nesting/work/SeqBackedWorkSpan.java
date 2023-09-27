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

import java.util.Iterator;
import java.util.List;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/** Logical sub-sequence of `WORK_ITEM`s, backed for simplicity's sake by an in-memory collection */
@NoArgsConstructor
@RequiredArgsConstructor
public class SeqBackedWorkSpan<WORK_ITEM> implements Workload.WorkSpan<WORK_ITEM> {

  @NonNull
  private List<WORK_ITEM> elems;
  // CAUTION: despite the "warning: @NonNull is meaningless on a primitive @lombok.RequiredArgsConstructor"...
  // if removed, no two-arg ctor is generated, so syntax error on `new CollectionBackedTaskSpan(elems, startIndex)`
  @NonNull
  private int startingIndex;
  private transient Iterator<WORK_ITEM> statefulDelegatee = null;

  @Override
  public int getNumElems() {
    return elems.size();
  }

  @Override
  public boolean hasNext() {
    if (statefulDelegatee == null) {
      statefulDelegatee = elems.iterator();
    }
    return statefulDelegatee.hasNext();
  }

  @Override
  public WORK_ITEM next() {
    if (statefulDelegatee == null) {
      throw new IllegalStateException("first call `hasNext()`!");
    }
    return statefulDelegatee.next();
  }

  @Override
  public String toString() {
    return getClassNickname() + "(" + startingIndex + "... {+" + getNumElems() + "})";
  }

  protected String getClassNickname() {
    // return getClass().getSimpleName();
    return "WorkSpan";
  }
}
