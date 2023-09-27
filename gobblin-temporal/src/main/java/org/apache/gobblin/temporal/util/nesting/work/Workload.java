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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Iterator;
import java.util.Optional;


/**
 * `Workload` models a logical collection of homogenous inputs over which a "foreach" operation can asynchronously apply
 * an arbitrary procedure to each element.  This encapsulates "processing" the entire collection of sequential
 * "work item" specifications by the uniform application of the chosen procedure(s).
 *
 * Given Temporal's required determinism, the work items and work spans should remain unchanged, with stable sequential
 * ordering.  This need not constrain `Workload`s to eager, advance elaboration: "streaming" definition is possible,
 * so long as producing a deterministic result.
 *
 * A actual, real-world workload might correspond to datastore contents, such as records serialized into HDFS files
 * or ordered DB query results.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class") // to handle impls
public interface Workload<WORK_ITEM> {

  /**
   * @return a sequential sub-sequence, from `startIndex` (0-based), unless it falls beyond the underlying sequence
   * NOTE: this is a blocking call that forces elaboration: `WorkSpan.getNumElems() < numElements` signifies end of seq
   */
  Optional<WorkSpan<WORK_ITEM>> getSpan(int startIndex, int numElements);

  /** Non-blocking, best-effort advice: to support non-strict elaboration, does NOT guarantee `index` will not exceed */
  boolean isIndexKnownToExceed(int index);

  default boolean isDefiniteSize() {
    return false;
  }

  /** Logical sub-sequence 'slice' of contiguous work items */
  public interface WorkSpan<T> extends Iterator<T> {
    int getNumElems();
  }
}
