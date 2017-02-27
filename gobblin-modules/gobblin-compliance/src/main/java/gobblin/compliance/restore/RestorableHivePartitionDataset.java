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

import static gobblin.compliance.purger.HivePurgerQueryTemplate.getDropPartitionQuery;
import static gobblin.compliance.purger.HivePurgerQueryTemplate.getUseDbQuery;


/**
 * A Restorable {@link HivePartitionDataset}
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
    String restorePolicyClass = this.state.getProp(ComplianceConfigurationKeys.RESTORE_POLICY_CLASS);
    this.restorePolicy =
        GobblinConstructorUtils.invokeConstructor(HivePartitionRestorePolicy.class, restorePolicyClass, this.state);
    try {
      this.datasetToRestore = (HivePartitionDataset) this.restorePolicy.getDatasetToRestore(this);
      log.info("Found dataset to restore with " + this.datasetToRestore.datasetURN());
    } catch (IOException e) {
      Throwables.propagate(e);
    }
    this.datasetOwner = getOwner();
    this.datasetToRestoreOwner = this.datasetToRestore.getOwner();
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_OWNER));
    this.trashOwner = Optional.fromNullable(this.state.getProp(ComplianceConfigurationKeys.TRASH_OWNER));
    setTimeStamp();
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
      log.info("Moving data for dataset " + datasetURN() + " from " + getLocation() + " to trash location "
          + trashPartitionLocation);
      this.datasetOwnerFs.mkdirs(trashPartitionLocation.getParent());
      this.datasetOwnerFs.rename(getLocation(), trashPartitionLocation);
      FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE);
      HadoopUtils
          .setPermissions(trashPartitionLocation.getParent(), this.datasetOwner, this.trashOwner, this.datasetOwnerFs,
              permission);

      log.info("Moving data from backup " + this.datasetToRestore.getLocation() + " to location " + getLocation());
      this.datasetOwnerFs.mkdirs(getLocation().getParent());
      this.datasetOwnerFs.rename(this.datasetToRestore.getLocation(), getLocation().getParent());
      HadoopUtils.setPermissions(getLocation().getParent(), this.datasetOwner, this.trashOwner, this.datasetOwnerFs,
          permission);
      executeDropTableQueries(queryExecutor);
    }
  }

  private void executeTrashTableQueries(HiveProxyQueryExecutor queryExecutor)
      throws IOException {
    String trashTableName = getTrashTableName();
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_DB));
    String trashDbName = this.state.getProp(ComplianceConfigurationKeys.TRASH_DB);
    String dbName = getDbName();
    String tableName = getTableName();
    String trashTableLocation = getTrashTableLocation();
    String trashPartitionLocation = getTrashPartitionLocation().toString();
    try {
      queryExecutor.executeQuery(HivePurgerQueryTemplate.getUseDbQuery(trashDbName), this.trashOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate
              .getCreateTableQuery(trashDbName + "." + trashTableName, dbName, tableName, trashTableLocation),
          this.trashOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate
          .getAddPartitionQuery(trashTableName, PartitionUtils.getPartitionSpecString(getSpec())), this.trashOwner);
      queryExecutor.executeQuery(HivePurgerQueryTemplate
          .getAlterTableLocationQuery(trashTableName, PartitionUtils.getPartitionSpecString(getSpec()),
              trashPartitionLocation), this.trashOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void executeDropTableQueries(HiveProxyQueryExecutor queryExecutor)
      throws IOException {
    String dbName = this.datasetToRestore.getDbName();
    String tableName = this.datasetToRestore.getTableName();
    String partitionSpec = PartitionUtils.getPartitionSpecString(this.datasetToRestore.getSpec());
    try {
      queryExecutor.executeQuery(getUseDbQuery(dbName), this.datasetToRestoreOwner);
      queryExecutor.executeQuery(getDropPartitionQuery(tableName, partitionSpec), this.datasetToRestoreOwner);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private String getTrashTableName() {
    String trashTableName = getCompleteTableName();
    return trashTableName + ComplianceConfigurationKeys.TRASH + this.timeStamp;
  }

  private void setTimeStamp() {
    this.timeStamp = Long.toString(System.currentTimeMillis());
  }

  private String getCompleteTableName() {
    return getDbName() + ComplianceConfigurationKeys.DBNAME_SEPARATOR + getTableName();
  }

  private String getTrashTableLocation() {
    Preconditions.checkArgument(this.state.contains(ComplianceConfigurationKeys.TRASH_DIR));
    return this.state.getProp(ComplianceConfigurationKeys.TRASH_DIR) + getCompleteTableName();
  }

  private Path getTrashPartitionLocation() {
    String trashTableLocation = getTrashTableLocation();
    return new Path(trashTableLocation + "/" + this.timeStamp + "/" + getName());
  }
}
