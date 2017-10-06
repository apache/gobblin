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

package org.apache.gobblin.metastore;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.gobblin.dataset.DatasetsFinder;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.metastore.predicates.DatasetPredicate;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;
import org.apache.gobblin.metastore.predicates.StoreNamePredicate;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * A {@link DatasetsFinder} to find {@link DatasetStoreDataset}s.
 */
public class DatasetStoreDatasetFinder implements DatasetsFinder<DatasetStoreDataset> {

  public static final String STORE_NAME_FILTER = "datasetStoreDatasetFinder.filter.storeName";
  public static final String DATASET_URN_FILTER = "datasetStoreDatasetFinder.filter.datasetUrn";

  private final Config config;
  private final DatasetStateStore store;
  private final StateStorePredicate predicate;

  public DatasetStoreDatasetFinder(FileSystem fs, Properties props) throws IOException {
    this.config = ConfigFactory.parseProperties(props);
    this.store = DatasetStateStore.buildDatasetStateStore(this.config);
    this.predicate = buildPredicate();
  }

  public DatasetStoreDatasetFinder(Properties props) throws IOException {
    this(FileSystem.get(new Configuration()), props);
  }

  private StateStorePredicate buildPredicate() {
    StateStorePredicate predicate = null;
    String storeName = null;
    String datasetUrn;

    if (ConfigUtils.hasNonEmptyPath(this.config, STORE_NAME_FILTER)) {
      storeName = this.config.getString(STORE_NAME_FILTER);
      predicate = new StoreNamePredicate(storeName, x -> true);
    }

    if (ConfigUtils.hasNonEmptyPath(this.config, DATASET_URN_FILTER)) {
      if (storeName == null) {
        throw new IllegalArgumentException(
            DATASET_URN_FILTER + " requires " + STORE_NAME_FILTER + " to also be defined.");
      }
      datasetUrn = this.config.getString(DATASET_URN_FILTER);
      predicate = new DatasetPredicate(storeName, datasetUrn, x -> true);
    }

    return predicate == null ? new StateStorePredicate(x -> true) : predicate;
  }

  @Override
  public List<DatasetStoreDataset> findDatasets() throws IOException {
    List<DatasetStateStoreEntryManager> entries = this.store.getMetadataForTables(this.predicate);

    Map<DatasetStoreDataset.Key, List<DatasetStateStoreEntryManager>> entriesGroupedByDataset =
        entries.stream().collect(Collectors.groupingBy(DatasetStoreDataset.Key::new));

    return entriesGroupedByDataset.entrySet().stream().
        map(entry -> new DatasetStoreDataset(entry.getKey(), entry.getValue())).collect(Collectors.toList());
  }

  @Override
  public Path commonDatasetRoot() {
    return null;
  }
}
