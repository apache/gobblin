/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.FsStateStore;


/**
 * A custom extension to {@link FsStateStore} for storing and reading {@link JobState.DatasetState}s.
 *
 * <p>
 *   The purpose of having this class is to hide some implementation details that are unnecessarily
 *   exposed if using the {@link FsStateStore} to store and serve job/dataset states between job runs.
 * </p>
 *
 * <p>
 *   In addition to persisting and reading {@link JobState.DatasetState}s. This class is also able to
 *   read job state files of existing jobs that store serialized instances of {@link JobState} for
 *   backward compatibility.
 * </p>
 *
 * @author ynli
 */
public class FsDatasetStateStore extends FsStateStore<JobState.DatasetState> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FsDatasetStateStore.class);

  static final String DATASET_STATE_STORE_TABLE_SUFFIX = ".jst";

  static final String CURRENT_DATASET_STATE_FILE_SUFFIX = "current";

  public FsDatasetStateStore(String fsUri, String storeRootDir) throws IOException {
    super(fsUri, storeRootDir, JobState.DatasetState.class);
  }

  public FsDatasetStateStore(FileSystem fs, String storeRootDir) throws IOException {
    super(fs, storeRootDir, JobState.DatasetState.class);
  }

  public FsDatasetStateStore(String storeUrl) throws IOException {
    super(storeUrl, JobState.DatasetState.class);
  }

  @Override
  public JobState.DatasetState get(String storeName, String tableName, String stateId) throws IOException {
    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath)) {
      return null;
    }

    Closer closer = Closer.create();
    try {
      SequenceFile.Reader reader = closer.register(new SequenceFile.Reader(this.fs, tablePath, this.conf));
      // This is necessary for backward compatibility as existing jobs are using the JobState class
      Writable writable = reader.getValueClass() == JobState.class ? new JobState() : new JobState.DatasetState();

      try {
        Text key = new Text();

        while (reader.next(key, writable)) {
          if (key.toString().equals(stateId)) {
            if (writable instanceof JobState.DatasetState) {
              return (JobState.DatasetState) writable;
            } else {
              return ((JobState) writable).newDatasetState(true);
            }
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    return null;
  }

  @Override
  public List<JobState.DatasetState> getAll(String storeName, String tableName) throws IOException {
    List<JobState.DatasetState> states = Lists.newArrayList();

    Path tablePath = new Path(new Path(this.storeRootDir, storeName), tableName);
    if (!this.fs.exists(tablePath)) {
      return states;
    }

    Closer closer = Closer.create();
    try {
      SequenceFile.Reader reader = closer.register(new SequenceFile.Reader(this.fs, tablePath, this.conf));
      // This is necessary for backward compatibility as existing jobs are using the JobState class
      Writable writable = reader.getValueClass() == JobState.class ? new JobState() : new JobState.DatasetState();

      try {
        Text key = new Text();
        while (reader.next(key, writable)) {
          if (writable instanceof JobState.DatasetState) {
            states.add((JobState.DatasetState) writable);
            writable = new JobState.DatasetState();
          } else {
            states.add(((JobState) writable).newDatasetState(true));
            writable = new JobState();
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } catch (Throwable t) {
      throw closer.rethrow(t);
    } finally {
      closer.close();
    }

    return states;
  }

  @Override
  public List<JobState.DatasetState> getAll(String storeName) throws IOException {
    return super.getAll(storeName);
  }

  /**
   * Get a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s.
   *
   * @param jobName the job name
   * @return a {@link Map} from dataset URNs to the latest {@link JobState.DatasetState}s
   * @throws IOException if there's something wrong reading the {@link JobState.DatasetState}s
   */
  public Map<String, JobState.DatasetState> getLatestDatasetStatesByUrns(String jobName) throws IOException {
    Path stateStorePath = new Path(this.storeRootDir, jobName);
    if (!this.fs.exists(stateStorePath)) {
      return ImmutableMap.of();
    }

    FileStatus[] stateStoreFileStatuses = this.fs.listStatus(stateStorePath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX);
      }
    });

    if (stateStoreFileStatuses == null || stateStoreFileStatuses.length == 0) {
      return ImmutableMap.of();
    }

    Map<String, JobState.DatasetState> datasetStatesByUrns = Maps.newHashMap();
    for (FileStatus stateStoreFileStatus : stateStoreFileStatuses) {
      List<JobState.DatasetState> previousDatasetStates = getAll(jobName, stateStoreFileStatus.getPath().getName());
      if (!previousDatasetStates.isEmpty()) {
        // There should be a single dataset state on the list if the list is not empty
        JobState.DatasetState previousDatasetState = previousDatasetStates.get(0);
        datasetStatesByUrns.put(previousDatasetState.getProp(ConfigurationKeys.DATASET_URN_KEY,
            ConfigurationKeys.DEFAULT_DATASET_URN), previousDatasetState);
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
    String alias = Strings.isNullOrEmpty(datasetUrn) ?
        CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX
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

  private String getAliasName(String datasetUrn) {
    return Strings.isNullOrEmpty(datasetUrn) ?
        CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX :
        datasetUrn + "-" + CURRENT_DATASET_STATE_FILE_SUFFIX + DATASET_STATE_STORE_TABLE_SUFFIX;
  }
}
