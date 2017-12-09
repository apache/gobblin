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
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * A {@link DatasetsFinder} that can return the {@link Dataset}s as an {@link Iterator}. This allows {@link Dataset}s
 * to be created on demand instead of all at once, possibly reducing memory usage and improving performance.
 */
public interface IterableDatasetFinder<T extends Dataset> extends DatasetsFinder<T> {

  /**
   * @return An {@link Iterator} over the {@link Dataset}s found.
   * @throws IOException
   * @deprecated use {@link #getDatasetsStream} instead.
   */
  @Deprecated
  public Iterator<T> getDatasetsIterator() throws IOException;

  /**
   * Get a stream of {@link Dataset}s found.
   * @param desiredCharacteristics desired {@link java.util.Spliterator} characteristics of this stream. The returned
   *                               stream need not satisfy these characteristics, this argument merely implies that the
   *                               caller will run optimally when those characteristics are present, allowing pushdown of
   *                               those characteristics. For example {@link java.util.Spliterator#SORTED} can sometimes
   *                               be pushed down at a cost, so the {@link DatasetsFinder} would only push it down if it is valuable
   *                               for the caller.
   * @param suggestedOrder suggested order of the datasets in the stream. Implementation may or may not return the entries
   *                       in that order. If the entries are in that order, implementation should ensure the spliterator
   *                       is annotated as such.
   * @return a stream of {@link Dataset}s found.
   * @throws IOException
   */
  default Stream<T> getDatasetsStream(int desiredCharacteristics, Comparator<T> suggestedOrder) throws IOException {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(getDatasetsIterator(), 0), false);
  }

}
