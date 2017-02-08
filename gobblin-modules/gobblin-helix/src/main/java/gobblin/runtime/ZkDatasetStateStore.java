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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.util.StateStoreTableInfo;
import gobblin.metastore.ZkStateStore;
import gobblin.runtime.util.DatasetUrnSanitizer;

import javax.annotation.Nullable;


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
  private static final Pattern DATASET_URN_PATTERN = Pattern.compile("\\A(?:(.+)-)?.+(\\..?)?\\z");
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkDatasetStateStore.class);

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
    Map<Optional<String>, String> latestDatasetStateFilePathsByUrns = getLatestDatasetStateFilePathsByUrns(jobName);
    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();
    for (Map.Entry<Optional<String>, String> filePath : latestDatasetStateFilePathsByUrns.entrySet()) {
      List<JobState.DatasetState> previousDatasetStates = getAll(jobName, filePath.getValue());
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

  /**
   * Get the latest {@link JobState.DatasetState} of a given dataset.
   *
   * @param storeName the name of the dataset state store
   * @param datasetUrn the dataset URN
   * @return the latest {@link JobState.DatasetState} of the dataset or {@link null} if it is not found
   * @throws IOException
   */
  public JobState.DatasetState getLatestDatasetState(String storeName, String datasetUrn) throws IOException {
    Optional<String> sanitizedDatasetUrn = DatasetUrnSanitizer.sanitize(datasetUrn);
    String tableName = (sanitizedDatasetUrn.isPresent() ?
        sanitizedDatasetUrn.get() + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + StateStoreTableInfo.CURRENT_NAME :
        StateStoreTableInfo.CURRENT_NAME) + DATASET_STATE_STORE_TABLE_SUFFIX;
    return get(storeName, tableName, datasetUrn);
  }

  /**
   * Persist a given {@link JobState.DatasetState}.
   *
   * @param datasetUrn the dataset URN
   * @param datasetState the {@link JobState.DatasetState} to persist
   * @throws IOException if there's something wrong persisting the {@link JobState.DatasetState}
   */
  public void persistDatasetState(String datasetUrn, JobState.DatasetState datasetState) throws IOException {
    String jobName = datasetState.getJobName();
    String jobId = datasetState.getJobId();

    Optional<String> sanitizedDatasetUrn = DatasetUrnSanitizer.sanitize(datasetUrn);
    String tableName = (sanitizedDatasetUrn.isPresent() ?
        sanitizedDatasetUrn.get() + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + jobId : jobId) +
        DATASET_STATE_STORE_TABLE_SUFFIX;
    LOGGER.info("Persisting " + tableName + " to the job state store");
    put(jobName, tableName, datasetState);
  }

  private Map<Optional<String>, String> getLatestDatasetStateFilePathsByUrns(String storeName) throws IOException {
    Iterable<String> tableNames = this.getTableNames(storeName, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return input.endsWith(DATASET_STATE_STORE_TABLE_SUFFIX);
      }
    });

    Map<Optional<String>, String> datasetStateFilePathsByUrns = Maps.newHashMap();
    for (String tableName : tableNames) {
      Matcher matcher = DATASET_URN_PATTERN.matcher(tableName);
      if (matcher.find()) {
        Optional<String> datasetUrn = DatasetUrnSanitizer.sanitize(matcher.group(1));
        if (!datasetStateFilePathsByUrns.containsKey(datasetUrn)) {
          LOGGER.debug("Latest table for {} dataset set to {}", datasetUrn.or("DEFAULT"), tableName);
          datasetStateFilePathsByUrns.put(datasetUrn, tableName);
        } else {
          String previousTableName = datasetStateFilePathsByUrns.get(datasetUrn);
          if (tableName.compareTo(previousTableName) > 0) {
            LOGGER.debug("Latest table for {} dataset set to {} instead of {}", datasetUrn.or("DEFAULT"), tableName, previousTableName);
            datasetStateFilePathsByUrns.put(datasetUrn, tableName);
          } else {
            LOGGER.debug("Latest table for {} dataset left as {}. Table {} is being ignored", datasetUrn.or("DEFAULT"), previousTableName, tableName);
          }
        }
      }
    }

    return datasetStateFilePathsByUrns;
  }
}