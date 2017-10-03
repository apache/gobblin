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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.typesafe.config.Config;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;

import lombok.Getter;


public interface DatasetStateStore<T extends State> extends StateStore<T> {
  String DATASET_STATE_STORE_TABLE_SUFFIX = ".jst";
  String CURRENT_DATASET_STATE_FILE_SUFFIX = "current";

  Pattern TABLE_NAME_PARSER_PATTERN = Pattern.compile("^(?:(.+)-)?([^-]+)\\.jst$");

  interface Factory {
    <T extends State> DatasetStateStore<T> createStateStore(Config config);
  }

  public Map<String, T> getLatestDatasetStatesByUrns(String jobName) throws IOException;

  public T getLatestDatasetState(String storeName, String datasetUrn) throws IOException;

  public void persistDatasetState(String datasetUrn, T datasetState) throws IOException;

  public void persistDatasetURNs(String storeName, Collection<String> datasetUrns) throws IOException;

  @Override
  default List<? extends DatasetStateStoreEntryManager> getMetadataForTables(StateStorePredicate predicate)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  default String sanitizeDatasetStatestoreNameFromDatasetURN(String storeName, String datasetURN) throws IOException {
    return datasetURN;
  }

  static String buildTableName(DatasetStateStore store, String storeName, String stateId, String datasetUrn) throws IOException {
    return Strings.isNullOrEmpty(datasetUrn) ? stateId + DATASET_STATE_STORE_TABLE_SUFFIX
        : store.sanitizeDatasetStatestoreNameFromDatasetURN(storeName,datasetUrn) + "-" + stateId + DATASET_STATE_STORE_TABLE_SUFFIX;
  }

  @Getter
  class TableNameParser {
    private final String sanitizedDatasetUrn;
    private final String stateId;

    public TableNameParser(String tableName) {
      Matcher matcher = TABLE_NAME_PARSER_PATTERN.matcher(tableName);
      if (matcher.matches()) {
        this.sanitizedDatasetUrn = matcher.group(1);
        this.stateId = matcher.group(2);
      } else {
        throw new IllegalArgumentException("Cannot parse table name " + tableName);
      }
    }
  }

  static DatasetStateStore buildDatasetStateStore(Config config) throws IOException {
    ClassAliasResolver<Factory> resolver =
        new ClassAliasResolver<>(DatasetStateStore.Factory.class);

    String stateStoreType = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_TYPE_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_TYPE);

    try {
      DatasetStateStore.Factory stateStoreFactory =
          resolver.resolveClass(stateStoreType).newInstance();

      return stateStoreFactory.createStateStore(config);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
