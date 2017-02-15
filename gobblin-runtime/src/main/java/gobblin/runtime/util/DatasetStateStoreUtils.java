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

package gobblin.runtime.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.util.StateStoreUtils;
import gobblin.metastore.util.StateStoreTableInfo;
import gobblin.runtime.JobState;
import gobblin.util.Id;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DatasetStateStoreUtils {
  private DatasetStateStoreUtils() {
  }

  /**
   * Get a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s.
   *
   * @param jobName the job name
   * @return a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s
   * @throws IOException if there's something wrong reading the {@link JobState.DatasetState}s
   */
  public static Map<String, JobState.DatasetState> getLatestDatasetStatesByUrns(
      DatasetStateStore<JobState.DatasetState> stateStore, String jobName) throws IOException {
    Map<Optional<String>, String> latestDatasetStateFilePathsByUrns = stateStore.getLatestDatasetStateTablesByUrn(jobName);
    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();
    for (Map.Entry<Optional<String>, String> filePath : latestDatasetStateFilePathsByUrns.entrySet()) {
      List<JobState.DatasetState> previousDatasetStates = stateStore.getAll(jobName, filePath.getValue());
      if (!previousDatasetStates.isEmpty()) {
        // There should be a single dataset state on the list if the list is not empty
        JobState.DatasetState previousDatasetState = previousDatasetStates.get(0);
        previousDatasetState.setDatasetStateId(filePath.getValue());
        datasetStatesByUrns.put(previousDatasetState.getDatasetUrn(), previousDatasetState);
      }
    }

    // The dataset (job) state from the deprecated "current.jst" will be read even though
    // the job has transitioned to the new dataset-based mechanism
    if (datasetStatesByUrns.size() > 1) {
      datasetStatesByUrns.remove(ConfigurationKeys.DEFAULT_DATASET_URN);
    }

    return datasetStatesByUrns;
  }

  public static Map<Optional<String>, String> getLatestDatasetStateTablesByUrn(
      DatasetStateStore<JobState.DatasetState> stateStore, String jobName) throws IOException {
    Iterable<String> tableNames = stateStore.getTableNames(jobName, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return input != null && input.endsWith(DatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX);
      }
    });

    String jobPrefix = StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + Id.Job.create(jobName).toString();
    Map<Optional<String>, String> datasetStateFilePathsByUrns = Maps.newHashMap();
    for (String tableName : tableNames) {
      int jobPrefixIndex = tableName.lastIndexOf(jobPrefix);
      Optional<String> datasetUrn = Optional.absent();
      if (jobPrefixIndex > 1) {
        datasetUrn = DatasetUrnSanitizer.sanitize(tableName.substring(0, jobPrefixIndex));
      }
      if (!datasetStateFilePathsByUrns.containsKey(datasetUrn)) {
        log.debug("Latest table for {} dataset set to {}", datasetUrn.or("DEFAULT"), tableName);
        datasetStateFilePathsByUrns.put(datasetUrn, tableName);
      } else {
        String previousTableName = datasetStateFilePathsByUrns.get(datasetUrn);
        Id currentJobId = StateStoreUtils.getId(datasetUrn.orNull(), tableName);
        Id previousJobId = StateStoreUtils.getId(datasetUrn.orNull(), previousTableName);
        if (currentJobId.getSequence().compareTo(previousJobId.getSequence()) > 0) {
          log.debug("Latest table for {} dataset set to {} instead of {}", datasetUrn.or("DEFAULT"), tableName, previousTableName);
          datasetStateFilePathsByUrns.put(datasetUrn, tableName);
        } else {
          log.debug("Latest table for {} dataset left as {}. Key {} is being ignored", datasetUrn.or("DEFAULT"), previousTableName, tableName);
        }
      }
    }

    return datasetStateFilePathsByUrns;
  }

  /**
   * Get the latest {@link JobState.DatasetState} of a given dataset.
   *
   * @param storeName the name of the dataset state store
   * @param datasetUrn the dataset URN
   * @return the latest {@link JobState.DatasetState} of the dataset or {@link null} if it is not found
   * @throws IOException
   */
  public static JobState.DatasetState getLatestDatasetState(DatasetStateStore<JobState.DatasetState> stateStore,
      String storeName, String datasetUrn) throws IOException {
    Optional<String> sanitizedDatasetUrn = DatasetUrnSanitizer.sanitize(datasetUrn);
    String tableName = (sanitizedDatasetUrn.isPresent() ?
            sanitizedDatasetUrn.get() + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + StateStoreTableInfo.CURRENT_NAME :
            StateStoreTableInfo.CURRENT_NAME) + DatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX;
    return stateStore.get(storeName, tableName, datasetUrn);
  }

  /**
   * Persist a given {@link JobState.DatasetState}.
   *
   * @param datasetUrn the dataset URN
   * @param datasetState the {@link JobState.DatasetState} to persist
   * @throws IOException if there's something wrong persisting the {@link JobState.DatasetState}
   */
  public static void persistDatasetState(DatasetStateStore<JobState.DatasetState> stateStore,
       String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    String jobName = datasetState.getJobName();
    String jobId = datasetState.getJobId();

    Optional<String> sanitizedDatasetUrn = DatasetUrnSanitizer.sanitize(datasetUrn);
    String tableName = (sanitizedDatasetUrn.isPresent() ?
            sanitizedDatasetUrn.get() + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + jobId : jobId) +
            DatasetStateStore.DATASET_STATE_STORE_TABLE_SUFFIX;
    log.info("Persisting " + tableName + " to the job state store");
    stateStore.put(jobName, tableName, datasetState);
  }
}
