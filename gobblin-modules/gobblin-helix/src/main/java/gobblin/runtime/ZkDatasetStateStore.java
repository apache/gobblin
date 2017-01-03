/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.runtime;

import com.google.common.base.CharMatcher;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import gobblin.annotation.Alias;
import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.ZkStateStore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger LOGGER = LoggerFactory.getLogger(ZkDatasetStateStore.class);
  private static final String CURRENT_SUFFIX = CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX;

  @Alias("zk")
  public static class Factory implements DatasetStateStore.Factory {
    @Override
    public DatasetStateStore<JobState.DatasetState> createStateStore(Properties props) {

      String connectString = props.getProperty(ConfigurationKeys.STATE_STORE_ZK_CONNECT_STRING_KEY);
      String rootDir = props.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);
      boolean compressedValues =
          Boolean.parseBoolean(props.getProperty(ConfigurationKeys.STATE_STORE_COMPRESSED_VALUES_KEY,
              Boolean.toString(ConfigurationKeys.DEFAULT_STATE_STORE_COMPRESSED_VALUES)));

      try {
        return new ZkDatasetStateStore(connectString, rootDir, compressedValues);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

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
    List<JobState.DatasetState> previousDatasetStates = getAll(jobName, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return input.endsWith(CURRENT_SUFFIX);
      }});

    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();
    if (!previousDatasetStates.isEmpty()) {
      JobState.DatasetState previousDatasetState = previousDatasetStates.get(0);
      datasetStatesByUrns.put(previousDatasetState.getDatasetUrn(), previousDatasetState);
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
    String alias =
        Strings.isNullOrEmpty(datasetUrn) ? CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX
            : datasetUrn + "-" + CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX;
    return get(storeName, alias, datasetUrn);
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

    datasetUrn = CharMatcher.is(':').replaceFrom(datasetUrn, '.');
    String tableName = Strings.isNullOrEmpty(datasetUrn) ? jobId + DATASET_STATE_STORE_TABLE_SUFFIX
        : datasetUrn + "-" + jobId + DATASET_STATE_STORE_TABLE_SUFFIX;
    LOGGER.info("Persisting " + tableName + " to the job state store");

    put(jobName, tableName, datasetState);
    createAlias(jobName, tableName, getAliasName(datasetUrn));
  }

  private static String getAliasName(String datasetUrn) {
    return Strings.isNullOrEmpty(datasetUrn) ? CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX
        : datasetUrn + "-" + CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX;
  }
}
