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
package gobblin.compliance.restore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.metadata.Partition;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import lombok.extern.slf4j.Slf4j;

import gobblin.compliance.ComplianceConfigurationKeys;
import gobblin.compliance.HivePartitionDataset;
import gobblin.compliance.HiveProxyQueryExecutor;
import gobblin.compliance.purger.HivePurgerQueryTemplate;
import gobblin.compliance.utils.PartitionUtils;
import gobblin.compliance.utils.ProxyUtils;
import gobblin.configuration.State;
import gobblin.util.HadoopUtils;
import gobblin.util.reflection.GobblinConstructorUtils;


/**
 * A Restorable {@link HivePartitionDataset}. It restores a given {@link HivePartitionDataset} with a
 * {@link gobblin.compliance.HivePartitionVersion} based on {@link RestorePolicy}
 *
 * @author adsharma
 */
@Slf4j
public class RestorableHivePartitionDataset extends HivePartitionDataset implements RestorableDataset {
  private HivePartitionDataset datasetToRestore;
  private HivePartitionRestorePolicy restorePolicy;
  private State state;
  private Optional<String> datasetOwner = Optional.absent();
  private Optional<String> datasetToRestoreOwner = Optional.absent();
  private Optional<String> trashOwner = Optional.absent();
  private FileSystem datasetOwnerFs;
  private String timeStamp;

  public RestorableHivePartitionDataset(Partition dataset, State state) {
    super(dataset);
    init(state);
  }

  public RestorableHivePartitionDataset(HivePartitionDataset hivePartitionDataset, State state) {
    super(hivePartitionDataset);
    init(state);
  }

  private void init(State state) {
    this.state = new State(state);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.RESTORE_POLICY_CLASS),
        "Missing required property " + ComplianceConfigurationKeys.RESTORE_POLICY_CLASS);
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_OWNER),
        "Missing required property " + ComplianceConfigurationKeys.TRASH_OWNER);
    String restorePolicyClass = this.state.getProp(ComplianceConfigurationKeys.RESTORE_POLICY_CLASS);
    this.datasetOwner = getOwner();
    this.trashOwner = Optional.fromNullable(this.state.getProp(ComplianceConfigurationKeys.TRASH_OWNER));
    setTimeStamp();
    this.restorePolicy =
        GobblinConstructorUtils.invokeConstructor(HivePartitionRestorePolicy.class, restorePolicyClass, this.state);
    try {
      this.datasetToRestore = (HivePartitionDataset) this.restorePolicy.getDatasetToRestore(this);
      log.info("Found dataset to restore with " + this.datasetToRestore.datasetURN());
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    this.datasetToRestoreOwner = this.datasetToRestore.getOwner();
  }

  public void restore()
      throws IOException {
    State state = new State(this.state);
    this.datasetOwnerFs = ProxyUtils.getOwnerFs(state, this.datasetOwner);
    try (HiveProxyQueryExecutor queryExecutor = ProxyUtils
        .getQueryExecutor(state, this.datasetOwner, this.datasetToRestoreOwner, this.trashOwner)) {
      if (this.state.getPropAsBoolean(ComplianceConfigurationKeys.COMPLIANCE_JOB_SIMULATE,
          ComplianceConfigurationKeys.DEFAULT_COMPLIANCE_JOB_SIMULATE)) {
        log.info("Simulating restore of " + datasetURN() + " with " + this.datasetToRestore.datasetURN());
        return;
      }

      Path trashPartitionLocation = getTrashPartitionLocation();
      executeTrashTableQueries(queryExecutor);
      this.datasetOwnerFs.mkdirs(trashPartitionLocation.getParent());
      this.datasetOwnerFs.rename(getLocation(), trashPartitionLocation);
      FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE);
      HadoopUtils
          .setPermissions(trashPartitionLocation.getParent(), this.datasetOwner, this.trashOwner, this.datasetOwnerFs,
              permission);
      log.info(
          "Moved dataset " + datasetURN() + " from " + getLocation() + " to trash location " + trashPartitionLocation);
      fsMove(this.datasetToRestore.getLocation(), getLocation());
      HadoopUtils.setPermissions(getLocation().getParent(), this.datasetOwner, this.trashOwner, this.datasetOwnerFs,
          permission);
      log.info("Moved data from backup " + this.datasetToRestore.getLocation() + " to location " + getLocation());
      executeDropPartitionQueries(queryExecutor);
    }
  }

  private void executeTrashTableQueries(HiveProxyQueryExecutor queryExecutor)
      throws IOException {
    String trashTableName = getTrashTableName();
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_DB),
        "Missing required property " + ComplianceConfigurationKeys.TRASH_DB);
    String trashDbName = this.state.getProp(ComplianceConfigurationKeys.TRASH_DB);
    try {
      queryExecutor.executeQuery(HivePurgerQueryTemplate.getUseDbQuery(trashDbName), this.trashOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate
          .getCreateTableQuery(trashDbName + "." + trashTableName, getDbName(), getTableName(),
              getTrashTableLocation()), this.trashOwner);
      Optional<String> fileFormat = Optional.absent();
      if (this.state.getPropAsBoolean(ComplianceConfigurationKeys.SPECIFY_PARTITION_FORMAT,
          ComplianceConfigurationKeys.DEFAULT_SPECIFY_PARTITION_FORMAT)) {
        fileFormat = getFileFormat();
      }
      queryExecutor.executeQuery(HivePurgerQueryTemplate
          .getAddPartitionQuery(trashTableName, PartitionUtils.getPartitionSpecString(getSpec()), fileFormat,
              Optional.fromNullable(getTrashPartitionLocation().toString())), this.trashOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void executeDropPartitionQueries(HiveProxyQueryExecutor queryExecutor)
      throws IOException {
    String dbName = this.datasetToRestore.getDbName();
    String tableName = this.datasetToRestore.getTableName();
    String partitionSpec = PartitionUtils.getPartitionSpecString(this.datasetToRestore.getSpec());
    try {
      queryExecutor.executeQuery(HivePurgerQueryTemplate.getUseDbQuery(dbName), this.datasetToRestoreOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate.getDropPartitionQuery(tableName, partitionSpec),
          this.datasetToRestoreOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private String getTrashTableName() {
    return getCompleteTableName() + ComplianceConfigurationKeys.TRASH + this.timeStamp;
  }

  private void setTimeStamp() {
    this.timeStamp = Long.toString(System.currentTimeMillis());
  }

  private String getCompleteTableName() {
    return StringUtils.join(Arrays.asList(getDbName(), getTableName()), ComplianceConfigurationKeys.DBNAME_SEPARATOR);
  }

  private String getTrashTableLocation() {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_DIR),
        "Missing required property " + ComplianceConfigurationKeys.TRASH_DIR);
    return this.state.getProp(ComplianceConfigurationKeys.TRASH_DIR) + getCompleteTableName();
  }

  private Path getTrashPartitionLocation() {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_DIR),
        "Missing required property " + ComplianceConfigurationKeys.TRASH_DIR);
      return new Path(StringUtils.join(Arrays.asList(this.state.getProp(ComplianceConfigurationKeys.TRASH_DIR),
          Path.getPathWithoutSchemeAndAuthority(getLocation()).toString()), '/'));
  }

  private void fsMove(Path from, Path to)
      throws IOException {
      for (FileStatus fileStatus : this.datasetOwnerFs.listStatus(from)) {
        if (fileStatus.isFile()) {
          this.datasetOwnerFs.rename(fileStatus.getPath(), to);
        }
    }
  }
}
