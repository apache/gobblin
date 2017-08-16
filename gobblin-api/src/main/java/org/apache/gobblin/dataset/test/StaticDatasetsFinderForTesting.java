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

package org.apache.gobblin.dataset.test;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.IterableDatasetFinder;
import org.apache.hadoop.fs.Path;

import lombok.AllArgsConstructor;


/**
 * A {@link org.apache.gobblin.dataset.DatasetsFinder} that returns a predefined set of {@link Dataset}s for testing.
 */
@AllArgsConstructor
public class StaticDatasetsFinderForTesting implements IterableDatasetFinder<Dataset> {

  private final List<Dataset> datasets;

  @Override
  public List<Dataset> findDatasets() throws IOException {
    return this.datasets;
  }

  @Override
  public Path commonDatasetRoot() {
    return null;
  }

  @Override
  public Iterator<Dataset> getDatasetsIterator() throws IOException {
    return this.datasets.iterator();
  }

  @Override
  public Stream<Dataset> getDatasetsStream(int desiredCharacteristics, Comparator<Dataset> suggestedOrder)
      throws IOException {
    return this.datasets.stream();
  }
}
