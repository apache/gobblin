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
package gobblin.compliance.purger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.HiveProxyQueryExecutor;
import gobblin.compliance.utils.PartitionUtils;
import gobblin.compliance.utils.ProxyUtils;
import gobblin.configuration.State;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A purgeable representation of {@link HivePartitionDataset}
 *
 * @author adsharma
 */
@Getter
@Slf4j
@Setter
public class PurgeableHivePartitionDataset extends HivePartitionDataset implements PurgeableDataset {
  private String complianceIdTable;
  private String complianceId;
  private Boolean simulate;
  private String timeStamp;
  private String complianceField;
  private List<String> purgeQueries;
  private FileSystem datasetOwnerFs;
  private State state;
  private Optional<String> datasetOwner = Optional.absent();
  private long startTime;
  private long endTime;
  private Boolean specifyPartitionFormat;

  public PurgeableHivePartitionDataset(Partition partition) {
    super(partition);
  }

  public PurgeableHivePartitionDataset(HivePartitionDataset hivePartitionDataset) {
    super(hivePartitionDataset);
  }

  /**
   * This method is responsible for actual purging.
   *  - It first creates a staging table partition with the same schema as of original table partition.
   *  - Staging table partition is then populated by original table left outer joined with compliance id table.
   *
   *  - Alter query will then change the partition location to the staging partition location.
   *  - In flight queries won't get affected due to alter partition query.
   */
  public void purge()
      throws IOException {
    this.datasetOwner = getOwner();
    State state = new State(this.state);
    this.datasetOwnerFs = ProxyUtils.getOwnerFs(state, this.datasetOwner);
    try (HiveProxyQueryExecutor queryExecutor = ProxyUtils.getQueryExecutor(state, this.datasetOwner)) {
      if (this.simulate) {
        log.info("Simulate is set to true. Wont't run actual queries");
        return;
      }
      String originalPartitionLocation = getOriginalPartitionLocation();

      // Create the staging table and staging partition
      queryExecutor.executeQueries(HivePurgerQueryTemplate.getCreateStagingTableQuery(this), this.datasetOwner);
      this.startTime = getLastModifiedTime(originalPartitionLocation);

      // Execute purge queries, that is insert filtered data into the staging partition
      queryExecutor.executeQueries(this.purgeQueries, this.datasetOwner);

      this.endTime = getLastModifiedTime(originalPartitionLocation);

      // Create a backup table and partition pointing to the original partition location
      queryExecutor.executeQueries(HivePurgerQueryTemplate.getBackupQueries(this), this.datasetOwner);

      String commitPolicyString = this.state.getProp(ComplianceConfigurationKeys.PURGER_COMMIT_POLICY_CLASS,
          ComplianceConfigurationKeys.DEFAULT_PURGER_COMMIT_POLICY_CLASS);
      CommitPolicy<PurgeableHivePartitionDataset> commitPolicy =
          GobblinConstructorUtils.invokeConstructor(CommitPolicy.class, commitPolicyString);
      if (!commitPolicy.shouldCommit(this)) {
        log.error("Last modified time before start of execution : " + this.startTime);
        log.error("Last modified time after execution of purge queries : " + this.endTime);
        throw new RuntimeException("Failed to commit. File modified during job run.");
      }

      // Alter the original table partition to start pointing to the cleaned-partition-location/staging-partition-location
      queryExecutor
          .executeQueries(HivePurgerQueryTemplate.getAlterOriginalPartitionLocationQueries(this), this.datasetOwner);

      // Drop the staging table
      queryExecutor.executeQueries(HivePurgerQueryTemplate.getDropStagingTableQuery(this), this.datasetOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private long getLastModifiedTime(String file)
      throws IOException {
    return this.datasetOwnerFs.getFileStatus(new Path(file)).getModificationTime();
  }

  public void setPurgeQueries(List<String> purgeQueries) {
    this.purgeQueries = purgeQueries;
  }

  public String getCompleteStagingTableName() {
    return getStagingDb() + "." + getStagingTableName();
  }

  public String getCompleteBackupTableName() {
    return getDbName() + "." + getBackupTableName();
  }

  public String getStagingTableName() {
    return getDbName() + ComplianceConfigurationKeys.DBNAME_SEPARATOR + getTableName()
        + ComplianceConfigurationKeys.STAGING + this.timeStamp;
  }

  public String getBackupTableName() {
    return getDbName() + ComplianceConfigurationKeys.DBNAME_SEPARATOR + getTableName()
        + ComplianceConfigurationKeys.BACKUP + this.timeStamp;
  }

  public String getStagingTableLocation() {
    return StringUtils.join(Arrays.asList(getTrashDir(), getStagingTableName()), '/');
  }

  public String getStagingPartitionLocation() {
    Path originalPartitionLocation = getLocation();
    if (PartitionUtils.isUnixTimeStamp(originalPartitionLocation.getName())) {
      return StringUtils.join(Arrays.asList(getLocation().getParent().toString(), this.timeStamp), '/');
    }
    else {
      return StringUtils.join(Arrays.asList(getLocation().toString(), this.timeStamp), '/');
    }
  }

  public String getOriginalPartitionLocation() {
    return getLocation().toString();
  }

  public String getStagingDb() {
    return getDbName();
  }

  public String getTrashDir() {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_DIR));
    return this.state.getProp(ComplianceConfigurationKeys.TRASH_DIR);
  }

  public String getBackupTableLocation() {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_DIR));
    return StringUtils.join(Arrays.asList(getTrashDir(), getBackupTableName()), '/');
  }
}
