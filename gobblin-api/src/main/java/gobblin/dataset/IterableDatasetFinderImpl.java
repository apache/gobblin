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

package gobblin.dataset;

import java.io.IOException;
import java.util.Iterator;

import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;


/**
 * Wraps a {@link DatasetsFinder} into an {@link IterableDatasetFinder}.
 */
@AllArgsConstructor
public class IterableDatasetFinderImpl<T extends Dataset> implements IterableDatasetFinder<T> {

  @Delegate
  private final DatasetsFinder<T> datasetFinder;

  @Override
  public Iterator<T> getDatasetsIterator()
      throws IOException {
    return this.datasetFinder.findDatasets().iterator();
  }
}
