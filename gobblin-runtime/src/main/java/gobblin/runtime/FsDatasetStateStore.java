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
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metastore.DatasetStateStore;
import gobblin.metastore.FsStateStore;
import gobblin.metastore.util.StateStoreTableInfo;
import gobblin.runtime.util.DatasetUrnSanitizer;
import gobblin.util.ConfigUtils;


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
 * @author Yinan Li
 */
public class FsDatasetStateStore extends FsStateStore<JobState.DatasetState>
    implements DatasetStateStore<JobState.DatasetState> {
  private static final Pattern DATASET_URN_PATTERN = Pattern.compile("\\A(?:(.+)-)?.+" +
          Pattern.quote(DATASET_STATE_STORE_TABLE_SUFFIX) +"\\z");
  private static final Logger LOGGER = LoggerFactory.getLogger(FsDatasetStateStore.class);

  protected static DatasetStateStore<JobState.DatasetState> createStateStore(Config config, String className) {
    // Add all job configuration properties so they are picked up by Hadoop
    Configuration conf = new Configuration();
    for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
      conf.set(entry.getKey(), entry.getValue().toString());
    }

    try {
      String stateStoreFsUri = ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_FS_URI_KEY,
          ConfigurationKeys.LOCAL_FS_URI);
      FileSystem stateStoreFs = FileSystem.get(URI.create(stateStoreFsUri), conf);
      String stateStoreRootDir = config.getString(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY);

      return (DatasetStateStore<JobState.DatasetState>) Class.forName(className)
          .getConstructor(FileSystem.class, String.class)
          .newInstance(stateStoreFs, stateStoreRootDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Failed to instantiate " + className, e);
    }
  }
  public FsDatasetStateStore(String fsUri, String storeRootDir) throws IOException {
    super(fsUri, storeRootDir, JobState.DatasetState.class);
    this.useTmpFileForPut = false;
  }

  public FsDatasetStateStore(FileSystem fs, String storeRootDir) throws IOException {
    super(fs, storeRootDir, JobState.DatasetState.class);
    this.useTmpFileForPut = false;
  }

  public FsDatasetStateStore(String storeUrl) throws IOException {
    super(storeUrl, JobState.DatasetState.class);
    this.useTmpFileForPut = false;
  }

  @Override
  public JobState.DatasetState get(String storeName, String tableName, String stateId) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(stateId), "State id is null or empty.");

    Path tablePath = getTablePath(storeName, tableName);
    if (tablePath == null || !this.fs.exists(tablePath)) {
      return null;
    }

    try (@SuppressWarnings("deprecation")
    SequenceFile.Reader reader = new SequenceFile.Reader(this.fs, tablePath, this.conf)) {
      // This is necessary for backward compatibility as existing jobs are using the JobState class
      Writable writable = reader.getValueClass() == JobState.class ? new JobState() : new JobState.DatasetState();

      try {
        Text key = new Text();

        while (reader.next(key, writable)) {
          if (key.toString().equals(stateId)) {
            if (writable instanceof JobState.DatasetState) {
              return (JobState.DatasetState) writable;
            }
            return ((JobState) writable).newDatasetState(true);
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    return null;
  }

  @Override
  public List<JobState.DatasetState> getAll(String storeName, String tableName) throws IOException {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(storeName), "Store name is null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName), "Table name is null or empty.");

    List<JobState.DatasetState> states = Lists.newArrayList();

    Path tablePath = getTablePath(storeName, tableName);
    if (tablePath == null || !this.fs.exists(tablePath)) {
      return states;
    }

    try (@SuppressWarnings("deprecation")
    SequenceFile.Reader reader = new SequenceFile.Reader(this.fs, tablePath, this.conf)) {
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
    }

    return states;
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
    String tableName = sanitizedDatasetUrn.isPresent() ?
        sanitizedDatasetUrn.get() + StateStoreTableInfo.TABLE_PREFIX_SEPARATOR + StateStoreTableInfo.CURRENT_NAME +
                DATASET_STATE_STORE_TABLE_SUFFIX :
        StateStoreTableInfo.CURRENT_NAME + DATASET_STATE_STORE_TABLE_SUFFIX;
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
    Path stateStorePath = new Path(this.storeRootDir, storeName);
    if (!this.fs.exists(stateStorePath)) {
      return Maps.newHashMap();
    }

    FileStatus[] stateStoreFileStatuses = this.fs.listStatus(stateStorePath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(DATASET_STATE_STORE_TABLE_SUFFIX);
      }
    });

    Map<Optional<String>, String> datasetStateFilePathsByUrns = Maps.newHashMap();
    for (FileStatus fileStatus : stateStoreFileStatuses) {
      String tableName = fileStatus.getPath().getName();
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
