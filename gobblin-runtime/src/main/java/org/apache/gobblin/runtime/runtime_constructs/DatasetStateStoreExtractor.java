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

package org.apache.gobblin.runtime.runtime_constructs;

import java.util.ListIterator;
import java.io.IOException;
import javax.annotation.Nullable;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.DatasetStoreDataset;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;

import lombok.extern.slf4j.Slf4j;


@Slf4j
/**
 * Using {@link DatasetStoreDataset} to extract its list of {@link DatasetStateStoreEntryManager},
 * which has the access to datasetURN and its belonging {@link DatasetStateStore}.
 */ public class DatasetStateStoreExtractor implements Extractor<String, ListIterator<DatasetStateStoreEntryManager>> {

  /**
   * A single dataset found by {@link org.apache.gobblin.metastore.DatasetStoreDatasetFinder}
   * can result in multiple {@link DatasetStateStoreEntryManager}s, in the case of Dataset level dataset-level state store.
   */
  private ListIterator<DatasetStateStoreEntryManager> iterableDatasetState;

  private DatasetStoreDataset dataset;

  public DatasetStateStoreExtractor(WorkUnitState workUnitState) {
    WorkUnit wu = workUnitState.getWorkunit();
    this.dataset =
        DatasetStateStoreSource.GENERICS_AWARE_GSON.fromJson(wu.getProp(DatasetStateStoreSource.ABSTRACT_DATASET),
            DatasetStoreDataset.class);

    for (DatasetStateStoreEntryManager datasetStateStoreEntryManager : this.dataset.getDatasetStateStoreMetadataEntries()) {
      iterableDatasetState.add(datasetStateStoreEntryManager);
    }
  }

  @Override
  public String getSchema() throws IOException {
    // Nothing to do here.
    return null;
  }

  @Override
  public long getExpectedRecordCount() {
    log.warn("Unsupported method: getExpectedRecordCount()");
    return -1;
  }

  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    // Nothing to do here.
  }

  @Nullable
  @Override
  public ListIterator<DatasetStateStoreEntryManager> readRecord(
      @Deprecated ListIterator<DatasetStateStoreEntryManager> reuse) throws DataRecordException, IOException {
    if (!iterableDatasetState.hasNext()) {
      return this.iterableDatasetState;
    } else {
      log.warn("There's no available record from " + "TODO: The name of StateStore");
      return null;
    }
  }
}
