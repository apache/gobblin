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

package org.apache.gobblin.data.management.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.util.PropertiesUtils;
import org.apache.gobblin.util.function.CheckedExceptionPredicate;


/**
 * A decorator for filtering datasets after a {@link DatasetsFinder} finds a {@link List} of {@link Dataset}s
 */
public class DatasetsFinderFilteringDecorator<T extends Dataset> implements DatasetsFinder<T> {
  private static final String PREFIX = "filtering.datasets.finder.";
  public static final String DATASET_CLASS = PREFIX + "class";
  public static final String ALLOWED = PREFIX + "allowed.predicates";
  public static final String DENIED = PREFIX + "denied.predicates";

  protected DatasetsFinder<T> datasetFinder;
  protected List<CheckedExceptionPredicate<T,IOException>> allowDatasetPredicates;
  protected List<CheckedExceptionPredicate<T,IOException>> denyDatasetPredicates;

  public DatasetsFinderFilteringDecorator(FileSystem fs, Properties properties) throws IOException {
    this.datasetFinder = DatasetUtils.instantiateDatasetFinder(
        DATASET_CLASS, properties, fs, DefaultFileSystemGlobFinder.class.getName());
    this.allowDatasetPredicates = instantiatePredicates(ALLOWED, properties);
    this.denyDatasetPredicates = instantiatePredicates(DENIED, properties);
  }

  @VisibleForTesting
  DatasetsFinderFilteringDecorator(
      DatasetsFinder<T> datasetsFinder,
      List<CheckedExceptionPredicate<T,IOException>> allowDatasetPredicates,
      List<CheckedExceptionPredicate<T,IOException>> denyDatasetPredicates) {
    this.datasetFinder = datasetsFinder;
    this.allowDatasetPredicates = allowDatasetPredicates;
    this.denyDatasetPredicates = denyDatasetPredicates;
  }

  @Override
  public List<T> findDatasets() throws IOException {
    List<T> datasets = datasetFinder.findDatasets();
    List<T> allowedDatasets = Collections.emptyList();
    try {
      allowedDatasets = datasets.parallelStream()
          .filter(dataset -> allowDatasetPredicates.stream()
              .map(CheckedExceptionPredicate::wrapToTunneled)
              .allMatch(p -> p.test(dataset)))
          .filter(dataset -> denyDatasetPredicates.stream()
              .map(CheckedExceptionPredicate::wrapToTunneled)
              .noneMatch(predicate -> predicate.test(dataset)))
          .collect(Collectors.toList());
    } catch (CheckedExceptionPredicate.WrappedIOException wrappedIOException) {
      wrappedIOException.rethrowWrapped();
    }

    return allowedDatasets;
  }

  @Override
  public Path commonDatasetRoot() {
    return datasetFinder.commonDatasetRoot();
  }

  private List<CheckedExceptionPredicate<T,IOException>> instantiatePredicates(String key, Properties props)
      throws IOException {
    List<CheckedExceptionPredicate<T,IOException>> predicates = new ArrayList<>();
    try {
      for (String className : PropertiesUtils.getPropAsList(props, key)) {
        predicates.add((CheckedExceptionPredicate<T, IOException>)
            ConstructorUtils.invokeConstructor(Class.forName(className), props));
      }

      return predicates;
    } catch (ReflectiveOperationException e) {
      throw new IOException(e);
    }
  }
}
