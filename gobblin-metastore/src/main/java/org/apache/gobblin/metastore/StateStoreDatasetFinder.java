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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.metadata.StateStoreEntryManager;
import org.apache.gobblin.metastore.predicates.DatasetPredicate;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;
import org.apache.gobblin.metastore.predicates.StoreNamePredicate;
import org.apache.gobblin.state.StatesFinder;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A {@link StatesFinder} to find {@link StateStoreDataset}s.
 */
public class StateStoreDatasetFinder implements StatesFinder<State> {

  private static final String STORE_NAME_FILTER = "datasetStoreDatasetFinder.filter.storeName";
  private static final String DATASET_URN_FILTER = "datasetStoreDatasetFinder.filter.datasetUrn";
  private final Config config;
  private final StateStore store;
  private final StateStorePredicate predicate;

  public StateStoreDatasetFinder(Properties props) {
    this.config = ConfigFactory.parseProperties(props);
    this.store = build(this.config);
    this.predicate = buildPredicate();
  }

  private StateStore build(Config config) {
    ClassAliasResolver<StateStore.Factory> resolver = new ClassAliasResolver<>(StateStore.Factory.class);

    String stateStoreType = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_TYPE_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_TYPE);

    try {
      StateStore.Factory stateStoreFactory = resolver.resolveClass(stateStoreType).newInstance();
      return stateStoreFactory.createStateStore(config, State.class);
    } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
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
  public List<StateStoreDataset> findStates() throws IOException {
    List<StateStoreEntryManager> entries = this.store.getMetadataForTables(this.predicate);
    Collectors.groupingBy(StateStoreDataset.Key::new);

    Map<StateStoreDataset.Key, List<StateStoreEntryManager>> entriesGroupedByDataset =
        entries.stream().collect(Collectors.groupingBy(StateStoreDataset.Key::new));

    return entriesGroupedByDataset.entrySet().stream().
        map(entry -> new StateStoreDataset(entry.getKey(), entry.getValue())).collect(Collectors.toList());
  }
}
