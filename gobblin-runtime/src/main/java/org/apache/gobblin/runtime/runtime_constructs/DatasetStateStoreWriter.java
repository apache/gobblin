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

import java.io.IOException;
import java.util.ListIterator;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.DatasetStateStore;
import org.apache.gobblin.metastore.metadata.DatasetStateStoreEntryManager;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.MysqlDatasetStateStore;
import org.apache.gobblin.runtime.metastore.filesystem.FsDatasetStateStoreEntryManager;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.writer.DataWriter;

import lombok.extern.slf4j.Slf4j;

/**
 * Use the {@link org.apache.gobblin.runtime.MysqlDatasetStateStore} to interact with MySQL for
 * writing {@link JobState.DatasetState} into MySQL.
 */
@Slf4j
public class DatasetStateStoreWriter implements DataWriter<ListIterator<DatasetStateStoreEntryManager>> {


  public static final String KEEP_ORIGINAL_STATE_STORE = "keep.original.state.store" ;
  public static final boolean DEFAULT_POLICY_ORIGINAL_STATE_STORE = true;

  public static final String DESTINATION_STATE_STORE_TYPE = "destination.state.store.type";
  /**
   * TODO: Remove this hardcode as
   * {@link org.apache.gobblin.runtime.metastore.filesystem.FsDatasetStateStoreEntryManager}
   */
  public static final String SOURCE_STATE_STORE_TYPE = "source.state.store.type";

  private DatasetStateStore _datasetStateStore ;

  /**
   * After write to new DatasetStateStore whether to delete the entry in old DatasetStateStore.
   * By default we keep it for archive purpose.
   */
  private boolean isOldStateStoreKept;

  /**
   *
   * @param state Properties obtained from {@link DatasetStateStoreWriterBuilder}
   */
  public DatasetStateStoreWriter(State state) {
    state.getPropAsBoolean(KEEP_ORIGINAL_STATE_STORE, DEFAULT_POLICY_ORIGINAL_STATE_STORE);

    String datasetStateStoreType = state.getProp(DESTINATION_STATE_STORE_TYPE, "mysql");
    try {
      ClassAliasResolver<DatasetStateStore.Factory> resolver = new ClassAliasResolver<>(DatasetStateStore.Factory.class);
      DatasetStateStore.Factory stateStoreFactory = resolver.resolveClass(datasetStateStoreType).newInstance();

      /**
       * props contains initial job configuration, which should contains everything related to
       * {@link MysqlDatasetStateStore}
       */
      this._datasetStateStore = stateStoreFactory.createStateStore(ConfigUtils.propertiesToConfig(state.getProperties()));
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Cannot not properly instantiate a DatasetStateStore object", e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(ListIterator<DatasetStateStoreEntryManager> record) throws IOException {
    while (record.hasNext()) {
      FsDatasetStateStoreEntryManager fsDatasetStateStoreEntryManager = (FsDatasetStateStoreEntryManager) (record.next());
      this._datasetStateStore.persistDatasetState(fsDatasetStateStoreEntryManager.getSanitizedDatasetUrn(),
          fsDatasetStateStoreEntryManager.readState());
      log.info(fsDatasetStateStoreEntryManager.getSanitizedDatasetUrn() + " of "
          + fsDatasetStateStoreEntryManager.getStoreName() + "-" + fsDatasetStateStoreEntryManager.getTableName()
          + " is migrated into new statestore");

      if (isOldStateStoreKept) {
        fsDatasetStateStoreEntryManager.delete();
      }
    }
  }

  @Override
  public void commit() throws IOException {

  }

  @Override
  public void cleanup() throws IOException {

  }

  @Override
  public long recordsWritten() {
    return 0;
  }

  @Override
  public long bytesWritten() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {

  }
}
