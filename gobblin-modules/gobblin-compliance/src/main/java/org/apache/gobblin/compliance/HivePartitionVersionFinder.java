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
package gobblin.compliance;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.retention.ComplianceRetentionJob;
import gobblin.compliance.retention.HivePartitionRetentionVersion;
import gobblin.configuration.State;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.dataset.Dataset;
import gobblin.util.AutoReturnableObject;


/**
 * A version finder class to find {@link HivePartitionVersion}s.
 *
 * @author adsharma
 */
@Slf4j
public class HivePartitionVersionFinder implements gobblin.data.management.version.finder.VersionFinder<HivePartitionVersion> {
  protected final FileSystem fs;
  protected final State state;
  protected List<String> patterns;
  private Optional<String> owner = Optional.absent();
  private List<HivePartitionVersion> versions = new ArrayList<>();
  private static final Object lock = new Object();
  private static final Splitter At_SPLITTER = Splitter.on("@").omitEmptyStrings().trimResults();

  public HivePartitionVersionFinder(FileSystem fs, State state, List<String> patterns) {
    this.fs = fs;
    this.state = new State(state);
    this.patterns = patterns;
  }

  @Override
  public Class<HivePartitionVersion> versionClass() {
    return HivePartitionVersion.class;
  }

  /**
   * Will find all the versions of the {@link HivePartitionDataset}.
   *
   * For a dataset with table name table1, corresponding versions table will be
   * table1_backup_timestamp or table1_staging_timestamp or table1_trash_timestamp
   *
   * Based on pattern, a type of version will be selected eg. backup or trash or staging
   *
   * If a Hive version's table contain no Partitions, it will be dropped.
   */
  @Override
  public Collection<HivePartitionVersion> findDatasetVersions(Dataset dataset)
      throws IOException {
    List<HivePartitionVersion> versions = new ArrayList<>();
    if (!(dataset instanceof HivePartitionDataset)) {
      return versions;
    }
    HivePartitionDataset hivePartitionDataset = (HivePartitionDataset) dataset;
    this.owner = hivePartitionDataset.getOwner();
    Preconditions.checkArgument(!this.patterns.isEmpty(),
        "No patterns to find versions for the dataset " + dataset.datasetURN());

    versions
        .addAll(findVersions(hivePartitionDataset.getName(), hivePartitionDataset.datasetURN()));
    return versions;
  }

  private List<HivePartitionVersion> findVersions(String name, String urn)
      throws IOException {
    State state = new State(this.state);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.HIVE_VERSIONS_WHITELIST),
        "Missing required property " + ComplianceConfigurationKeys.HIVE_VERSIONS_WHITELIST);

    state.setProp(ComplianceConfigurationKeys.HIVE_DATASET_WHITELIST,
        this.state.getProp(ComplianceConfigurationKeys.HIVE_VERSIONS_WHITELIST));
    setVersions(name, state);
    log.info("Found " + this.versions.size() + " versions for the dataset " + urn);
    return this.versions;
  }

  private void addPartitionsToVersions(List<HivePartitionVersion> versions, String name,
      List<Partition> partitions)
      throws IOException {
    for (Partition partition : partitions) {
      if (partition.getName().equalsIgnoreCase(name)) {
        versions.add(new HivePartitionRetentionVersion(partition));
      }
    }
  }

  private void setVersions(final String name, final State state)
      throws IOException {
    try {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      loginUser.doAs(new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run()
            throws IOException {
          synchronized (lock) {
            List<Partition> partitions = null;
            for (String tableName : ComplianceRetentionJob.tableNamesList) {
              for (String pattern : patterns) {
                if (tableName.contains(pattern)) {
                  partitions = getPartitions(tableName);
                  addPartitionsToVersions(versions, name, partitions);
                }
              }
            }
          }
          return null;
        }
      });
    } catch (InterruptedException | IOException e) {
      throw new IOException(e);
    }
  }

  private static List<Partition> getPartitions(String completeTableName) {
    List<String> tableList = At_SPLITTER.splitToList(completeTableName);
    if (tableList.size() != 2) {
      log.warn("Invalid table name " + completeTableName);
      return Collections.EMPTY_LIST;
    }
    try (AutoReturnableObject<IMetaStoreClient> client = ComplianceRetentionJob.pool.getClient()) {
      Table table = client.get().getTable(tableList.get(0), tableList.get(1));
      HiveDataset dataset = new HiveDataset(FileSystem.newInstance(new Configuration()), ComplianceRetentionJob.pool,
          new org.apache.hadoop.hive.ql.metadata.Table(table), new Properties());
      return dataset.getPartitionsFromDataset();
    } catch (IOException | TException e) {
      log.warn("Unable to get Partitions for table " + completeTableName + " " + e.getMessage());
    }
    return Collections.EMPTY_LIST;
  }
}
