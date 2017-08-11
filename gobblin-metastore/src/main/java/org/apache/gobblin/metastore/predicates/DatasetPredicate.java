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

package org.apache.gobblin.metastore.predicates;

import java.io.IOException;

import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.metastore.metadata.StateStoreEntryManager;

import com.google.common.base.Predicate;


/**
 * A {@link StateStorePredicate} used to select only entries from a {@link org.apache.gobblin.metastore.DatasetStateStore}
 * with the provided dataset urn.
 */
public class DatasetPredicate extends StoreNamePredicate {

  private final String datasetUrn;

  public DatasetPredicate(String storeName, String datasetUrn, Predicate<StateStoreEntryManager> customPredicate) {
    super(storeName, customPredicate);
    this.datasetUrn = datasetUrn;
  }

  @Override
  public boolean apply(StateStoreEntryManager input) {
    if (!(input instanceof DatasetStateStoreEntryManager)) {
      return false;
    }
    DatasetStateStoreEntryManager datasetStateStoreEntryMetadata = (DatasetStateStoreEntryManager) input;
    try {
      return super.apply(input) && datasetStateStoreEntryMetadata.getStateStore().
          sanitizeDatasetStatestoreNameFromDatasetURN(getStoreName(), this.datasetUrn).
          equals(((DatasetStateStoreEntryManager) input).getSanitizedDatasetUrn());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
