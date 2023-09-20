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

package org.apache.gobblin.temporal.workflows;

import java.util.Iterator;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import lombok.NoArgsConstructor;
import lombok.NonNull;


/** Logical sub-sequence of `Task`s, backed for simplicity's sake by an in-memory collection */
@NoArgsConstructor
public class CollectionBackedTaskSpan<T> implements Workload.TaskSpan<T> {
    @NonNull
    private List<T> elems;
    private int startingIndex;
    private transient Iterator<T> statefulDelegatee = null;

    public CollectionBackedTaskSpan(@NotNull List<T> elems, int startingIndex) {
        this.elems = elems;
        this.startingIndex = startingIndex;
    }

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
    public T next() {
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
        return "TaskSpan";
    }
}
