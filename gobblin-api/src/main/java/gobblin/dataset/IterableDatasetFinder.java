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

package gobblin.dataset;

import java.io.IOException;
import java.util.Iterator;


/**
 * A {@link DatasetsFinder} that can return the {@link Dataset}s as an {@link Iterator}. This allows {@link Dataset}s
 * to be created on demand instead of all at once, possibly reducing memory usage and improving performance.
 */
public interface IterableDatasetFinder<T extends Dataset> extends DatasetsFinder<T> {

  /**
   * @return An {@link Iterator} over the {@link Dataset}s found.
   * @throws IOException
   */
  public Iterator<T> getDatasetsIterator() throws IOException;

}
