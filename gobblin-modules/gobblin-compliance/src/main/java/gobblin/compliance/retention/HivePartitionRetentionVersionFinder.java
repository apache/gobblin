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
package gobblin.compliance.retention;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.HivePartitionVersion;
import gobblin.compliance.HivePartitionVersionFinder;
import gobblin.compliance.HiveProxyQueryExecutor;
import gobblin.compliance.purger.HivePurgerQueryTemplate;
import gobblin.compliance.utils.ProxyUtils;
import gobblin.configuration.State;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.dataset.Dataset;


/**
 * A version finder class to find versions corresponding to the {@link HivePartitionDataset}.
 *
 * @author adsharma
 */
@Slf4j
public class HivePartitionRetentionVersionFinder extends HivePartitionVersionFinder {

  private Optional<String> owner = Optional.absent();
  private List<HivePartitionVersion> versions = new ArrayList<>();

  public HivePartitionRetentionVersionFinder(FileSystem fs, State state, List<String> patterns) {
    super(fs, state, patterns);
  }

  /**
   * Will find all the versions of the {@link HivePartitionDataset}.
   * Versions will be the corresponding Partitions in the backup db.
   *
   * For a dataset with table name table1, corresponding versions table will be
   * table1_backup_timestamp or table1_staging_timestamp
   *
   * If a Hive versions table contains no Partitions, it will be dropped.
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
        .addAll(findVersionsFromPatterns(hivePartitionDataset.getName(), hivePartitionDataset.datasetURN(), patterns));
    return versions;
  }

  private List<HivePartitionVersion> findVersionsFromPatterns(String name, String urn, List<String> patterns)
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

  private void addPartitionsToVersions(List<HivePartitionVersion> versions, String name, HiveDataset hiveDataset,
      List<Partition> partitions)
      throws IOException {
    if (partitions.isEmpty()) {
      if (Boolean.parseBoolean(this.state.getProp(ComplianceConfigurationKeys.SHOULD_DROP_EMPTY_TABLES,
          ComplianceConfigurationKeys.DEFAULT_SHOULD_DROP_EMPTY_TABLES))) {
        executeQueries(hiveDataset);
      }
      return;
    }
    for (Partition partition : partitions) {
      if (partition.getName().equalsIgnoreCase(name)) {
        versions.add(new HivePartitionRetentionVersion(partition));
      }
    }
  }

  private void executeQueries(HiveDataset hiveDataset)
      throws IOException {
    String dbName = hiveDataset.getTable().getDbName();
    String tableName = hiveDataset.getTable().getTableName();
    Optional<String> datasetOwner = Optional.fromNullable(hiveDataset.getTable().getOwner());
    try (HiveProxyQueryExecutor hiveProxyQueryExecutor = ProxyUtils
        .getQueryExecutor(new State(this.state), datasetOwner)) {
      hiveProxyQueryExecutor.executeQuery(HivePurgerQueryTemplate.getDropTableQuery(dbName, tableName), datasetOwner);
    } catch (SQLException e) {
      throw new IOException(e);
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
          HiveDatasetFinder finder = new HiveDatasetFinder(fs, state.getProperties());
          for (HiveDataset hiveDataset : finder.findDatasets()) {
            List<Partition> partitions = hiveDataset.getPartitionsFromDataset();
            for (String pattern : patterns) {
              if (hiveDataset.getTable().getTableName().contains(pattern)) {
                addPartitionsToVersions(versions, name, hiveDataset, partitions);
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
}
