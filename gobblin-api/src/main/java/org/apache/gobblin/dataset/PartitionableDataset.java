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

package org.apache.gobblin.dataset;

import java.io.IOException;
import java.util.Comparator;
import java.util.stream.Stream;


/**
 * A {@link Dataset} that can be partitioned into disjoint subsets of the dataset.
 * @param <T> the type of partitions returned by the dataset.
 */
public interface PartitionableDataset<T extends PartitionableDataset.DatasetPartition> extends Dataset {

  /**
   * Get a stream of partitions.
   * @param desiredCharacteristics desired {@link java.util.Spliterator} characteristics of this stream. The returned
   *                               stream need not satisfy these characteristics, this argument merely implies that the
   *                               caller will run optimally when those characteristics are present, allowing pushdown of
   *                               those characteristics. For example {@link java.util.Spliterator#SORTED} can sometimes
   *                               be pushed down at a cost, so the {@link Dataset} would only push it down if it is valuable
   *                               for the caller.
   * @param suggestedOrder suggested order of the partitions in the stream. Implementation may or may not return the entries
   *                       in that order. If the entries are in that order, implementation should ensure the spliterator
   *                       is annotated as such.
   * @return a {@link Stream} over {@link DatasetPartition}s in this dataset.
   */
  Stream<T> getPartitions(int desiredCharacteristics, Comparator<T> suggestedOrder) throws IOException;

  /**
   * A partition of a {@link PartitionableDataset}.
   */
  interface DatasetPartition extends URNIdentified {
    /**
     * URN for this dataset.
     */
    String getUrn();

    /**
     * @return Dataset this partition belongs to.
     */
    Dataset getDataset();
  }

}
