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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionVersion;
import gobblin.compliance.HiveProxyQueryExecutor;
import gobblin.compliance.purger.HivePurgerQueryTemplate;
import gobblin.compliance.utils.PartitionUtils;
import gobblin.compliance.utils.ProxyUtils;
import gobblin.configuration.State;
import gobblin.data.management.retention.dataset.CleanableDataset;
import gobblin.data.management.version.DatasetVersion;
import gobblin.util.HadoopUtils;

import static gobblin.compliance.purger.HivePurgerQueryTemplate.getDropPartitionQuery;
import static gobblin.compliance.purger.HivePurgerQueryTemplate.getUseDbQuery;


/**
 * Class to move/clean backups/staging partitions.
 *
 * @author adsharma
 */
@Slf4j
public class HivePartitionVersionRetentionReaper extends HivePartitionVersionRetentionRunner {
  private FileSystem versionOwnerFs;

  private boolean simulate;
  private Optional<String> versionOwner = Optional.absent();
  private Optional<String> backUpOwner = Optional.absent();

  public HivePartitionVersionRetentionReaper(CleanableDataset dataset, DatasetVersion version,
      List<String> nonDeletableVersionLocations, State state) {
    super(dataset, version, nonDeletableVersionLocations, state);
    this.versionOwner = ((HivePartitionVersion) this.datasetVersion).getOwner();
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.BACKUP_OWNER),
        "Missing required property " + ComplianceConfigurationKeys.BACKUP_OWNER);
    this.backUpOwner = Optional.fromNullable(this.state.getProp(ComplianceConfigurationKeys.BACKUP_OWNER));
    this.simulate = this.state.getPropAsBoolean(ComplianceConfigurationKeys.COMPLIANCE_JOB_SIMULATE,
        ComplianceConfigurationKeys.DEFAULT_COMPLIANCE_JOB_SIMULATE);
  }

  /**
   * If simulate is set to true, will simply return.
   * If a version is pointing to a non-existing location, then drop the partition and close the jdbc connection.
   * If a version is pointing to the same location as of the dataset, then drop the partition and close the jdbc connection.
   * If a version is staging, it's data will be deleted and metadata is dropped.
   * IF a versions is backup, it's data will be moved to a backup dir, current metadata will be dropped and it will
   * be registered in the backup db.
   */
  @Override
  public void clean()
      throws IOException {
    Path versionLocation = ((HivePartitionRetentionVersion) this.datasetVersion).getLocation();
    Path datasetLocation = ((CleanableHivePartitionDataset) this.cleanableDataset).getLocation();
    String completeName = ((HivePartitionRetentionVersion) this.datasetVersion).datasetURN();
    State state = new State(this.state);

    this.versionOwnerFs = ProxyUtils.getOwnerFs(state, this.versionOwner);

    try (HiveProxyQueryExecutor queryExecutor = ProxyUtils
        .getQueryExecutor(state, this.versionOwner, this.backUpOwner)) {

      Path newVersionLocation = getNewVersionLocation();

      if (!this.versionOwnerFs.exists(versionLocation)) {
        log.info("Data versionLocation doesn't exist. Metadata will be dropped for the version  " + completeName);
      } else if (datasetLocation.toString().equalsIgnoreCase(versionLocation.toString())) {
        log.info(
            "Dataset location is same as version location. Won't delete the data but metadata will be dropped for the version "
                + completeName);
      } else if (this.simulate) {
        log.info("Simulate is set to true. Won't move the version " + completeName);
        return;
      } else if (completeName.contains(ComplianceConfigurationKeys.STAGING)) {
        log.info("Deleting data from version " + completeName);
        this.versionOwnerFs.delete(versionLocation, true);
      } else if (completeName.contains(ComplianceConfigurationKeys.BACKUP)) {
        executeAlterQueries(queryExecutor);
        log.info("Creating new dir " + newVersionLocation.getParent().toString());
        this.versionOwnerFs.mkdirs(newVersionLocation.getParent());
        log.info("Moving data from " + versionLocation + " to " + newVersionLocation);
        this.versionOwnerFs.rename(versionLocation, newVersionLocation);
        FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE);
        HadoopUtils
            .setPermissions(newVersionLocation.getParent(), this.versionOwner, this.backUpOwner, this.versionOwnerFs,
                permission);
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

  private void executeAlterQueries(HiveProxyQueryExecutor queryExecutor)
      throws IOException {
    HivePartitionRetentionVersion version = (HivePartitionRetentionVersion) this.datasetVersion;
    String partitionSpecString = PartitionUtils.getPartitionSpecString(version.getSpec());
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.BACKUP_DB),
        "Missing required property " + ComplianceConfigurationKeys.BACKUP_DB);
    String backUpDb = this.state.getProp(ComplianceConfigurationKeys.BACKUP_DB);
    String backUpTableName = getCompleteTableName(version);
    try {
      queryExecutor.executeQuery(HivePurgerQueryTemplate.getUseDbQuery(backUpDb), this.backUpOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate
          .getCreateTableQuery(backUpDb + "." + backUpTableName, version.getDbName(), version.getTableName(),
              getBackUpTableLocation(version)), this.backUpOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate.getAddPartitionQuery(backUpTableName, partitionSpecString),
          this.backUpOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate
              .getAlterTableLocationQuery(backUpTableName, partitionSpecString, getNewVersionLocation().toString()),
          this.backUpOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void executeDropVersionQueries(HiveProxyQueryExecutor queryExecutor)
      throws IOException {
    HivePartitionRetentionVersion version = (HivePartitionRetentionVersion) this.datasetVersion;
    String partitionSpec = PartitionUtils.getPartitionSpecString(version.getSpec());
    try {
      queryExecutor.executeQuery(getUseDbQuery(version.getDbName()), this.versionOwner);
      queryExecutor.executeQuery(getDropPartitionQuery(version.getTableName(), partitionSpec), this.versionOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private String getVersionTimeStamp() {
    return ((HivePartitionRetentionVersion) this.datasetVersion).getTimeStamp();
  }

  private String getCompleteTableName(HivePartitionVersion version) {
    return version.getTableName();
  }

  private String getBackUpTableLocation(HivePartitionVersion version) {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.BACKUP_DIR),
        "Missing required property " + ComplianceConfigurationKeys.BACKUP_DIR);
    return StringUtils
        .join(Arrays.asList(this.state.getProp(ComplianceConfigurationKeys.BACKUP_DIR), getCompleteTableName(version)),
            '/');
  }

  private Path getNewVersionLocation() {
    HivePartitionVersion version = (HivePartitionRetentionVersion) this.datasetVersion;
    String backUpTableLocation = getBackUpTableLocation(version);
    return new Path(
        StringUtils.join(Arrays.asList(backUpTableLocation, getVersionTimeStamp(), version.getName()), '/'));
  }
}
