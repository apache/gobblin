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

package gobblin.runtime;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Optional;

import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.ZkStateStore;
import gobblin.runtime.util.DatasetStateStoreUtils;


/**
 * A custom extension to {@link ZkStateStore} for storing and reading {@link JobState.DatasetState}s.
 *
 * <p>
 *   The purpose of having this class is to hide some implementation details that are unnecessarily
 *   exposed if using the {@link ZkStateStore} to store and serve dataset states between job runs.
 * </p>
 *
 */
public class ZkDatasetStateStore extends ZkStateStore<JobState.DatasetState>
    implements DatasetStateStore<JobState.DatasetState> {

  public ZkDatasetStateStore(String connectString, String storeRootDir, boolean compressedValues) throws IOException {
    super(connectString, storeRootDir, compressedValues, JobState.DatasetState.class);
  }

  /**
   * Get a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s.
   *
   * @param jobName the job name
   * @return a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s
   * @throws IOException if there's something wrong reading the {@link JobState.DatasetState}s
   */
  public Map<String, JobState.DatasetState> getLatestDatasetStatesByUrns(String jobName) throws IOException {
    return DatasetStateStoreUtils.getLatestDatasetStatesByUrns(this, jobName);
  }

  /**
   * Get the latest {@link JobState.DatasetState} of a given dataset.
   *
   * @param storeName the name of the dataset state store
   * @param datasetUrn the dataset URN
   * @return the latest {@link JobState.DatasetState} of the dataset or {@link null} if it is not found
   * @throws IOException
   */
  public JobState.DatasetState getLatestDatasetState(String storeName, String datasetUrn) throws IOException {
    return DatasetStateStoreUtils.getLatestDatasetState(this, storeName, datasetUrn);
  }

  /**
   * Persist a given {@link JobState.DatasetState}.
   *
   * @param datasetUrn the dataset URN
   * @param datasetState the {@link JobState.DatasetState} to persist
   * @throws IOException if there's something wrong persisting the {@link JobState.DatasetState}
   */
  public void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    DatasetStateStoreUtils.persistDatasetState(this, datasetUrn, datasetState);
  }

  public Map<Optional<String>, String> getLatestDatasetStateTablesByUrn(String jobName) throws IOException {
    return DatasetStateStoreUtils.getLatestDatasetStateTablesByUrn(this, jobName);
  }
}