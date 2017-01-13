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

package gobblin.hive.metastore;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HiveLock;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegProps;
import gobblin.hive.HiveRegister;
import gobblin.hive.HiveRegistrationUnit.Column;
import gobblin.hive.HiveTable;
import gobblin.hive.spec.HiveSpec;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.GobblinMetricsRegistry;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.util.AutoCloseableLock;
import gobblin.util.AutoReturnableObject;


/**
 * An implementation of {@link HiveRegister} that uses {@link IMetaStoreClient} for Hive registration.
 *
 * <p>
 *   An instance of this class is constructed with a {@link State} object or obtained via
 *   {@link HiveRegister#get(State)}. Property {@link HiveRegProps#HIVE_DB_ROOT_DIR} is required for registering
 *   a table or a partition if the database does not exist.
 * </p>
 *
 * <p>
 *   The {@link #register(HiveSpec)} method is asynchronous and returns immediately. Registration is performed in a
 *   thread pool whose size is controlled by {@link HiveRegProps#HIVE_REGISTER_THREADS}.
 * </p>
 *
 * @author Ziyang Liu
 */
@Slf4j
@Alpha
public class HiveMetaStoreBasedRegister extends HiveRegister {

  private final HiveMetastoreClientPool clientPool;
  private final HiveLock locks = new HiveLock();
  private final EventSubmitter eventSubmitter;

  public HiveMetaStoreBasedRegister(State state, Optional<String> metastoreURI) throws IOException {
    super(state);

    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(this.props.getNumThreads());
    config.setMaxIdle(this.props.getNumThreads());
    this.clientPool = HiveMetastoreClientPool.get(this.props.getProperties(), metastoreURI);

    MetricContext metricContext =
        GobblinMetricsRegistry.getInstance().getMetricContext(state, HiveMetaStoreBasedRegister.class, GobblinMetrics.getCustomTagsFromState(state));

    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "gobblin.hive.HiveMetaStoreBasedRegister").build();
  }

  @Override
  protected void registerPath(HiveSpec spec) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      Table table = HiveMetaStoreUtils.getTable(spec.getTable());

      createDbIfNotExists(client.get(), table.getDbName());
      createOrAlterTable(client.get(), table, spec);

      Optional<HivePartition> partition = spec.getPartition();
      if (partition.isPresent()) {
        addOrAlterPartition(client.get(), table, HiveMetaStoreUtils.getPartition(partition.get()), spec);
      }
      HiveMetaStoreEventHelper.submitSuccessfulPathRegistration(eventSubmitter, spec);
    } catch (TException e) {
      HiveMetaStoreEventHelper.submitFailedPathRegistration(eventSubmitter, spec, e);
      throw new IOException(e);
    }
  }

  @Override
  public boolean createDbIfNotExists(String dbName) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      return createDbIfNotExists(client.get(), dbName);
    }
  }

  private boolean createDbIfNotExists(IMetaStoreClient client, String dbName) throws IOException {
    Database db = new Database();
    db.setName(dbName);

    try (AutoCloseableLock lock = this.locks.getDbLock(dbName)) {
      try {
        client.getDatabase(db.getName());
        return false;
      } catch (NoSuchObjectException nsoe) {
        // proceed with create
      } catch (TException te) {
        throw new IOException(te);
      }

      Preconditions.checkState(this.hiveDbRootDir.isPresent(),
          "Missing required property " + HiveRegProps.HIVE_DB_ROOT_DIR);
      db.setLocationUri(new Path(this.hiveDbRootDir.get(), dbName + HIVE_DB_EXTENSION).toString());

      try {
        client.createDatabase(db);
        log.info("Created database " + dbName);
        HiveMetaStoreEventHelper.submitSuccessfulDBCreation(this.eventSubmitter, dbName);
        return true;
      } catch (AlreadyExistsException e) {
        return false;
      } catch (TException e) {
        HiveMetaStoreEventHelper.submitFailedDBCreation(this.eventSubmitter, dbName, e);
        throw new IOException("Unable to create Hive database " + dbName, e);
      }
    }
  }

  @Override
  public boolean createTableIfNotExists(HiveTable table) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        AutoCloseableLock lock = this.locks.getTableLock(table.getDbName(), table.getTableName())) {
      return createTableIfNotExists(client.get(), HiveMetaStoreUtils.getTable(table), table);
    }
  }

  @Override
  public boolean addPartitionIfNotExists(HiveTable table, HivePartition partition) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        AutoCloseableLock lock = this.locks.getTableLock(table.getDbName(), table.getTableName())) {
      try {
        client.get().getPartition(table.getDbName(), table.getTableName(), partition.getValues());
        return false;
      } catch (NoSuchObjectException e) {
        client.get().alter_partition(table.getDbName(), table.getTableName(),
            getPartitionWithCreateTimeNow(HiveMetaStoreUtils.getPartition(partition)));
        HiveMetaStoreEventHelper.submitSuccessfulPartitionAdd(this.eventSubmitter, table, partition);
        return true;
      }
    } catch (TException e) {
      HiveMetaStoreEventHelper.submitFailedPartitionAdd(this.eventSubmitter, table, partition, e);
      throw new IOException(String.format("Unable to add partition %s in table %s in db %s", partition.getValues(),
          table.getTableName(), table.getDbName()), e);
    }
  }

  private boolean createTableIfNotExists(IMetaStoreClient client, Table table, HiveTable hiveTable) throws IOException {
    String dbName = table.getDbName();
    String tableName = table.getTableName();

    try (AutoCloseableLock lock = this.locks.getTableLock(dbName, tableName)) {
      if (client.tableExists(table.getDbName(), table.getTableName())) {
        return false;
      }
      client.createTable(getTableWithCreateTimeNow(table));
      log.info(String.format("Created Hive table %s in db %s", tableName, dbName));
      HiveMetaStoreEventHelper.submitSuccessfulTableCreation(this.eventSubmitter, hiveTable);
      return true;
    } catch (TException e) {
      HiveMetaStoreEventHelper.submitFailedTableCreation(eventSubmitter, hiveTable, e);
      throw new IOException(String.format("Error in creating or altering Hive table %s in db %s", table.getTableName(),
          table.getDbName()), e);
    }
  }

  private void createOrAlterTable(IMetaStoreClient client, Table table, HiveSpec spec) throws TException {

    String dbName = table.getDbName();
    String tableName = table.getTableName();
    try (AutoCloseableLock lock = this.locks.getTableLock(dbName, tableName)) {
      try {
        client.createTable(getTableWithCreateTimeNow(table));
        log.info(String.format("Created Hive table %s in db %s", tableName, dbName));
      } catch (TException e) {
        try {
          HiveTable existingTable = HiveMetaStoreUtils.getHiveTable(client.getTable(dbName, tableName));
          if (needToUpdateTable(existingTable, spec.getTable())) {
            client.alter_table(dbName, tableName, getTableWithCreateTime(table, existingTable));
            log.info(String.format("updated Hive table %s in db %s", tableName, dbName));
          }
        } catch (TException e2) {
          log.error(
              String.format("Unable to create or alter Hive table %s in db %s: " + e2.getMessage(), tableName, dbName),
              e2);
          throw e2;
        }
      }
    }
  }

  @Override
  public boolean existsTable(String dbName, String tableName) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      return client.get().tableExists(dbName, tableName);
    } catch (TException e) {
      throw new IOException(String.format("Unable to check existence of table %s in db %s", tableName, dbName), e);
    }
  }

  @Override
  public boolean existsPartition(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      client.get().getPartition(dbName, tableName, partitionValues);
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      throw new IOException(String.format("Unable to check existence of partition %s in table %s in db %s",
          partitionValues, tableName, dbName), e);
    }
  }

  @Override
  public void dropTableIfExists(String dbName, String tableName) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      if (client.get().tableExists(dbName, tableName)) {
        client.get().dropTable(dbName, tableName);
        HiveMetaStoreEventHelper.submitSuccessfulTableDrop(eventSubmitter, dbName, tableName);
        log.info("Dropped table " + tableName + " in db " + dbName);
      }
    } catch (TException e) {
      HiveMetaStoreEventHelper.submitFailedTableDrop(eventSubmitter, dbName, tableName, e);
      throw new IOException(String.format("Unable to deregister table %s in db %s", tableName, dbName), e);
    }
  }

  @Override
  public void dropPartitionIfExists(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      client.get().dropPartition(dbName, tableName, partitionValues, false);
      HiveMetaStoreEventHelper.submitSuccessfulPartitionDrop(eventSubmitter, dbName, tableName, partitionValues);
      log.info("Dropped partition " + partitionValues + " in table " + tableName + " in db " + dbName);
    } catch (NoSuchObjectException e) {
      // Partition does not exist. Nothing to do
    } catch (TException e) {
      HiveMetaStoreEventHelper.submitFailedPartitionDrop(eventSubmitter, dbName, tableName, partitionValues, e);
      throw new IOException(String.format("Unable to check existence of Hive partition %s in table %s in db %s",
          partitionValues, tableName, dbName), e);
    }
  }

  private void addOrAlterPartition(IMetaStoreClient client, Table table, Partition partition, HiveSpec spec)
      throws TException {
    Preconditions.checkArgument(table.getPartitionKeysSize() == partition.getValues().size(),
        String.format("Partition key size is %s but partition value size is %s", table.getPartitionKeys().size(),
            partition.getValues().size()));

    try (AutoCloseableLock lock =
        this.locks.getPartitionLock(table.getDbName(), table.getTableName(), partition.getValues())) {

      try {
        client.add_partition(getPartitionWithCreateTimeNow(partition));
        log.info(String.format("Added partition %s to table %s with location %s", stringifyPartition(partition),
            table.getTableName(), partition.getSd().getLocation()));
      } catch (TException e) {
        try {
          HivePartition existingPartition = HiveMetaStoreUtils
              .getHivePartition(client.getPartition(table.getDbName(), table.getTableName(), partition.getValues()));

          if (needToUpdatePartition(existingPartition, spec.getPartition().get())) {
            log.info(String.format("Partition update required. ExistingPartition %s, newPartition %s",
                stringifyPartition(existingPartition), stringifyPartition(spec.getPartition().get())));
            Partition newPartition = getPartitionWithCreateTime(partition, existingPartition);
            log.info(String.format("Altering partition %s", newPartition));
            client.alter_partition(table.getDbName(), table.getTableName(), newPartition);
            log.info(String.format("Updated partition %s in table %s with location %s", stringifyPartition(newPartition),
                table.getTableName(), partition.getSd().getLocation()));
          } else {
            log.info(String.format("Partition %s in table %s with location %s already exists and no need to update",
                stringifyPartition(partition), table.getTableName(), partition.getSd().getLocation()));
          }
        } catch (Throwable e2) {
          log.error(String.format(
              "Unable to add or alter partition %s in table %s with location %s: " + e2.getMessage(),
              stringifyPartitionVerbose(partition), table.getTableName(), partition.getSd().getLocation()), e2);
          throw e2;
        }
      }
    }
  }

  private static String stringifyPartition(Partition partition) {
    if (log.isDebugEnabled()) {
      return stringifyPartitionVerbose(partition);
    }
    return Arrays.toString(partition.getValues().toArray());
  }

  private static String stringifyPartition(HivePartition partition) {
    return partition.toString();
  }

  private static String stringifyPartitionVerbose(Partition partition) {
    return partition.toString();
  }

  @Override
  public Optional<HiveTable> getTable(String dbName, String tableName) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      return Optional.of(HiveMetaStoreUtils.getHiveTable(client.get().getTable(dbName, tableName)));
    } catch (NoSuchObjectException e) {
      return Optional.<HiveTable> absent();
    } catch (TException e) {
      throw new IOException("Unable to get table " + tableName + " in db " + dbName, e);
    }
  }

  @Override
  public Optional<HivePartition> getPartition(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException {

    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      return Optional
          .of(HiveMetaStoreUtils.getHivePartition(client.get().getPartition(dbName, tableName, partitionValues)));
    } catch (NoSuchObjectException e) {
      return Optional.<HivePartition> absent();
    } catch (TException e) {
      throw new IOException(
          "Unable to get partition " + partitionValues + " from table " + tableName + " in db " + dbName, e);
    }
  }

  @Override
  public void alterTable(HiveTable table) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      if (!client.get().tableExists(table.getDbName(), table.getTableName())) {
        throw new IOException("Table " + table.getTableName() + " in db " + table.getDbName() + " does not exist");
      }
      client.get().alter_table(table.getDbName(), table.getTableName(),
          getTableWithCreateTimeNow(HiveMetaStoreUtils.getTable(table)));
      HiveMetaStoreEventHelper.submitSuccessfulTableAlter(eventSubmitter, table);
    } catch (TException e) {
      HiveMetaStoreEventHelper.submitFailedTableAlter(eventSubmitter, table, e);
      throw new IOException("Unable to alter table " + table.getTableName() + " in db " + table.getDbName(), e);
    }
  }

  @Override
  public void alterPartition(HiveTable table, HivePartition partition) throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      client.get().alter_partition(table.getDbName(), table.getTableName(),
          getPartitionWithCreateTimeNow(HiveMetaStoreUtils.getPartition(partition)));
      HiveMetaStoreEventHelper.submitSuccessfulPartitionAlter(eventSubmitter, table, partition);
    } catch (TException e) {
      HiveMetaStoreEventHelper.submitFailedPartitionAlter(eventSubmitter, table, partition, e);
      throw new IOException(String.format("Unable to alter partition %s in table %s in db %s", partition.getValues(),
          table.getTableName(), table.getDbName()), e);
    }
  }

  private Partition getPartitionWithCreateTimeNow(Partition partition) {
    return getPartitionWithCreateTime(partition, Ints.checkedCast(DateTime.now().getMillis() / 1000));
  }

  private Partition getPartitionWithCreateTime(Partition partition, HivePartition referencePartition) {
    return getPartitionWithCreateTime(partition,
        Ints.checkedCast(referencePartition.getCreateTime().or(DateTime.now().getMillis() / 1000)));
  }

  /**
   * Sets create time if not already set.
   */
  private Partition getPartitionWithCreateTime(Partition partition, int createTime) {
    if (partition.isSetCreateTime() && partition.getCreateTime() > 0) {
      return partition;
    }
    Partition actualPartition = partition.deepCopy();
    actualPartition.setCreateTime(createTime);
    return actualPartition;
  }

  private Table getTableWithCreateTimeNow(Table table) {
    return gettableWithCreateTime(table, Ints.checkedCast(DateTime.now().getMillis() / 1000));
  }

  private Table getTableWithCreateTime(Table table, HiveTable referenceTable) {
    return gettableWithCreateTime(table,
        Ints.checkedCast(referenceTable.getCreateTime().or(DateTime.now().getMillis() / 1000)));
  }

  /**
   * Sets create time if not already set.
   */
  private Table gettableWithCreateTime(Table table, int createTime) {
    if (table.isSetCreateTime() && table.getCreateTime() > 0) {
      return table;
    }
    Table actualtable = table.deepCopy();
    actualtable.setCreateTime(createTime);
    return actualtable;
  }

}
