/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.request_allocation;

import java.io.Serializable;
import java.util.Comparator;

import lombok.AllArgsConstructor;


/**
 * A {@link HierarchicalPrioritizer} built from two input {@link Comparator}s: one for {@link Requestor} and one
 * for {@link Request}.
 */
@AllArgsConstructor
public class SimpleHierarchicalPrioritizer<T extends Request<T>> implements HierarchicalPrioritizer<T>, Serializable {

  private final Comparator<Requestor<T>> requestorComparator;
  private final Comparator<T> requestComparator;

  @Override
  public int compareRequestors(Requestor<T> r1, Requestor<T> r2) {
    return this.requestorComparator.compare(r1, r2);
  }

  @Override
  public int compare(T o1, T o2) {
    int requestorComparison = this.requestorComparator.compare(o1.getRequestor(), o2.getRequestor());
    if (requestorComparison != 0) {
      return requestorComparison;
    }
    return this.requestComparator.compare(o1, o2);
  }
}
