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
package org.apache.gobblin.compliance.retention;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.compliance.ComplianceConfigurationKeys;
import org.apache.gobblin.compliance.HiveProxyQueryExecutor;
import org.apache.gobblin.compliance.purger.HivePurgerQueryTemplate;
import org.apache.gobblin.compliance.utils.PartitionUtils;
import org.apache.gobblin.compliance.utils.ProxyUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.data.management.retention.dataset.CleanableDataset;
import org.apache.gobblin.data.management.version.DatasetVersion;
import org.apache.gobblin.util.HadoopUtils;


/**
 * A version cleaner for the {@link HivePartitionRetentionVersion}.
 *
 * A version will be considered as clean only if it's metadata no longer exists in the db and the data
 * it was pointing to no longer exists.
 *
 * @author adsharma
 */
@Slf4j
public class HivePartitionVersionRetentionCleaner extends HivePartitionVersionRetentionRunner {
  private FileSystem fs;
  private boolean simulate;
  private Optional<String> versionOwner = Optional.absent();

  public HivePartitionVersionRetentionCleaner(CleanableDataset dataset, DatasetVersion version,
      List<String> nonDeletableVersionLocations, State state) {
    super(dataset, version, nonDeletableVersionLocations, state);
    this.versionOwner = ((HivePartitionRetentionVersion) this.datasetVersion).getOwner();
    this.simulate = this.state.getPropAsBoolean(ComplianceConfigurationKeys.COMPLIANCE_JOB_SIMULATE,
        ComplianceConfigurationKeys.DEFAULT_COMPLIANCE_JOB_SIMULATE);
  }

  /**
   * If simulate is set to true, this will simply return.
   * If version is pointing to an empty location, drop the partition and close the jdbc connection.
   * If version is pointing to the same location as of the dataset, then drop the partition and close the jdbc connection.
   * If version is pointing to the non deletable version locations, then drop the partition and close the jdbc connection.
   * Otherwise delete the data underneath, drop the partition and close the jdbc connection.
   */
  @Override
  public void clean()
      throws IOException {
    Path versionLocation = ((HivePartitionRetentionVersion) this.datasetVersion).getLocation();
    Path datasetLocation = ((CleanableHivePartitionDataset) this.cleanableDataset).getLocation();
    String completeName = ((HivePartitionRetentionVersion) this.datasetVersion).datasetURN();

    State state = new State(this.state);
    this.fs = ProxyUtils.getOwnerFs(state, this.versionOwner);

    try (HiveProxyQueryExecutor queryExecutor = ProxyUtils.getQueryExecutor(state, this.versionOwner)) {
      log.info("Trying to clean version " + completeName);
      if (!this.fs.exists(versionLocation)) {
        log.info("Data versionLocation doesn't exist. Metadata will be dropped for the version  " + completeName);
      } else if (datasetLocation.toString().equalsIgnoreCase(versionLocation.toString())) {
        log.info(
            "Dataset location is same as version location. Won't delete the data but metadata will be dropped for the version "
                + completeName);
      } else if (this.nonDeletableVersionLocations.contains(versionLocation.toString())) {
        log.info(
            "This version corresponds to the non deletable version. Won't delete the data but metadata will be dropped for the version "
                + completeName);
      } else if (HadoopUtils.hasContent(this.fs, versionLocation)) {
        if (this.simulate) {
          log.info("Simulate is set to true. Won't delete the partition " + completeName);
          return;
        }
        log.info("Deleting data from the version " + completeName);
        this.fs.delete(versionLocation, true);
      }
      executeDropVersionQueries(queryExecutor);
    }
  }

  // These methods are not implemented by this class
  @Override
  public void preCleanAction() {

  }

  @Override
  public void postCleanAction() {

  }

  private void executeDropVersionQueries(HiveProxyQueryExecutor queryExecutor)
      throws IOException {
    String dbName = ((HivePartitionRetentionVersion) this.datasetVersion).getDbName();
    String tableName = ((HivePartitionRetentionVersion) this.datasetVersion).getTableName();
    String partitionSpec =
        PartitionUtils.getPartitionSpecString(((HivePartitionRetentionVersion) this.datasetVersion).getSpec());
    try {
      queryExecutor.executeQuery(HivePurgerQueryTemplate.getUseDbQuery(dbName), this.versionOwner);
      queryExecutor
          .executeQuery(HivePurgerQueryTemplate.getDropPartitionQuery(tableName, partitionSpec), this.versionOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
