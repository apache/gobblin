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

import java.util.List;

import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;

import lombok.Data;


/**
 * A {@link Dataset} representing a group of entries in a {@link DatasetStateStore} with the same dataset urn.
 */
@Data
public class DatasetStoreDataset implements Dataset {

  private final Key key;
  private final List<DatasetStateStoreEntryManager> datasetStateStoreMetadataEntries;

  @Override
  public String datasetURN() {
    return this.key.getStoreName() + ":::" + this.key.getSanitizedDatasetUrn();
  }

  /**
   * The key for a {@link DatasetStoreDataset}.
   */
  @Data
  public static class Key {
    private final String storeName;
    private final String sanitizedDatasetUrn;

    public Key(DatasetStateStoreEntryManager metadata) {
      this.storeName = metadata.getStoreName();
      this.sanitizedDatasetUrn = metadata.getSanitizedDatasetUrn();
    }
  }

}
