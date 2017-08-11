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

package org.apache.gobblin.metastore.metadata;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.DatasetStateStore;

import lombok.Getter;


/**
 * A {@link StateStoreEntryManager} in a {@link DatasetStateStore}.
 */
@Getter
public abstract class DatasetStateStoreEntryManager<T extends State> extends StateStoreEntryManager<T> {

  /**
   * The sanitized dataset urn. Sanitization usually involves a one-way function on the dataset urn, so the actual
   * urn cannot be determined except by {@link #readState()}.
   */
  private final String sanitizedDatasetUrn;
  /**
   * An identifier for the state. Usually a job id or "current" for the latest state for that dataset.
   */
  private final String stateId;
  private final DatasetStateStore datasetStateStore;

  public DatasetStateStoreEntryManager(String storeName, String tableName, long timestamp,
      DatasetStateStore.TableNameParser tableNameParser, DatasetStateStore datasetStateStore) {
    this(storeName, tableName, timestamp, tableNameParser.getSanitizedDatasetUrn(), tableNameParser.getStateId(), datasetStateStore);
  }

  public DatasetStateStoreEntryManager(String storeName, String tableName, long timestamp, String sanitizedDatasetUrn,
      String stateId, DatasetStateStore datasetStateStore) {
    super(storeName, tableName, timestamp, datasetStateStore);
    this.sanitizedDatasetUrn = sanitizedDatasetUrn;
    this.stateId = stateId;
    this.datasetStateStore = datasetStateStore;
  }

  @Override
  public DatasetStateStore getStateStore() {
    return this.datasetStateStore;
  }
}
