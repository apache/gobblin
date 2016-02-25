/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.hive.metastore;

import java.io.IOException;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HiveMetaStoreClientFactory;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HivePartition;
import gobblin.hive.HiveRegProps;
import gobblin.hive.HiveRegister;
import gobblin.hive.HiveStripedLocks;
import gobblin.hive.HiveRegistrationUnit.Column;
import gobblin.hive.HiveTable;
import gobblin.hive.HiveStripedLocks.HiveLock;
import gobblin.hive.spec.HiveSpec;
import gobblin.util.AutoReturnableObject;

import lombok.extern.slf4j.Slf4j;


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
 * @author ziliu
 */
@Slf4j
@Alpha
public class HiveMetaStoreBasedRegister extends HiveRegister {

  private final HiveMetastoreClientPool clientPool;
  private final HiveStripedLocks locks = new HiveStripedLocks();

  public HiveMetaStoreBasedRegister(State state) throws IOException {
    this(state, Optional.<String>absent());
  }

  public HiveMetaStoreBasedRegister(State state, Optional<String> metastoreURI) throws IOException {
    super(state);

    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(this.props.getNumThreads());
    config.setMaxIdle(this.props.getNumThreads());
    this.clientPool = HiveMetastoreClientPool.get(this.props.getProperties(), metastoreURI);
  }

  @Override
  protected void registerPath(HiveSpec spec) throws IOException {
    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      Table table = HiveMetaStoreUtils.getTable(spec.getTable());
      createDbIfNotExists(client.get(), table.getDbName());

      Optional<HivePartition> partition = spec.getPartition();
      createOrAlterTable(client.get(), table);

      if (partition.isPresent()) {

        // Register a partition
        addOrAlterPartition(client.get(), table, HiveMetaStoreUtils.getPartition(partition.get()), spec.getPath());
      }
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean createDbIfNotExists(String dbName) throws IOException {
    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      return createDbIfNotExists(client.get(), dbName);
    }
  }

  private boolean createDbIfNotExists(IMetaStoreClient client, String dbName) throws IOException {
    Database db = new Database();
    db.setName(dbName);

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

    try(HiveLock lock = locks.getDbLock(dbName)) {
      client.createDatabase(db);
      log.info("Created database " + dbName);
      return true;
    } catch (AlreadyExistsException e) {
      return false;
    } catch (TException e) {
      throw new IOException("Unable to create Hive database " + dbName, e);
    }
  }

  @Override
  public boolean createTableIfNotExists(HiveTable table) throws IOException {

    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(table.getDbName(), table.getTableName())) {
      if (client.get().tableExists(table.getDbName(), table.getTableName())) {
        return false;
      }
      client.get().createTable(HiveMetaStoreUtils.getTable(table));
      return true;
    } catch (TException e) {
      throw new IOException(String.format("Error in creating or altering Hive table %s in db %s", table.getTableName(),
          table.getDbName()), e);
    }
  }

  @Override
  public boolean addPartitionIfNotExists(HiveTable table, HivePartition partition) throws IOException {

    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(table.getDbName(), table.getTableName())) {
      try {
        client.get().getPartition(table.getTableName(), table.getDbName(), partition.getValues());
        return false;
      } catch (NoSuchObjectException e) {
        client.get().alter_partition(table.getDbName(), table.getTableName(),
            HiveMetaStoreUtils.getPartition(partition));
        return true;
      }
    } catch (TException e) {
      throw new IOException(String.format("Unable to add partition %s in table %s in db %s", partition.getValues(),
          table.getTableName(), table.getDbName()), e);
    }
  }

  private void createOrAlterTable(IMetaStoreClient client, Table table) throws IOException {

    String dbName = table.getDbName();
    String tableName = table.getTableName();

    try (HiveLock lock = locks.getTableLock(dbName, tableName)){
      if (client.tableExists(dbName, tableName)) {
        Table existingTable = client.getTable(dbName, tableName);
        table.setCreateTime(existingTable.getCreateTime());
        table.setLastAccessTime(existingTable.getLastAccessTime());
        if (needToUpdateTable(existingTable, table)) {
          client.alter_table(dbName, tableName, table);
          log.info(String.format("updated Hive table %s in db %s", tableName, dbName));
        } else {
          log.info(String.format("Hive table %s in db %s exists and no need to update", tableName, dbName));
        }
      } else {
        client.createTable(table);
        log.info(String.format("Created Hive table %s in db %s", tableName, dbName));
      }
    } catch (TException e) {
      throw new IOException(String.format("Error in creating or altering Hive table %s in db %s", tableName, dbName),
          e);
    }
  }

  private boolean needToUpdateTable(Table existingTable, Table newTable) {
    return !existingTable.equals(newTable);
  }

  @Override
  public boolean existsTable(String dbName, String tableName) throws IOException {

    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(dbName, tableName)) {
      return client.get().tableExists(dbName, tableName);
    } catch (TException e) {
      throw new IOException(String.format("Unable to check existence of table %s in db %s", tableName, dbName), e);
    }
  }

  @Override
  public boolean existsPartition(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException {

    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(dbName, tableName)) {
      client.get().getPartition(tableName, dbName, partitionValues);
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

    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(dbName, tableName)) {
      if (client.get().tableExists(dbName, tableName)) {
        client.get().dropTable(dbName, tableName);
        log.info("Dropped table " + tableName + " in db " + dbName);
      }
    } catch (TException e) {
      throw new IOException(String.format("Unable to deregister table %s in db %s", tableName, dbName), e);
    }
  }

  @Override
  public void dropPartitionIfExists(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException {

    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(dbName, tableName)) {
      client.get().dropPartition(dbName, tableName, partitionValues, false);
      log.info("Dropped partition " + partitionValues + " in table " + tableName + " in db " + dbName);
    } catch (NoSuchObjectException e) {
      // Partition does not exist. Nothing to do
    } catch (TException e) {
      throw new IOException(String.format("Unable to check existence of Hive partition %s in table %s in db %s",
          partitionValues, tableName, dbName), e);
    }
  }

  private void addOrAlterPartition(IMetaStoreClient client, Table table, Partition partition, Path partitionLocation)
      throws TException {
    Preconditions.checkArgument(table.getPartitionKeysSize() == partition.getValues().size(),
        String.format("Partition key size is %s but partition value size is %s", table.getPartitionKeys().size(),
            partition.getValues().size()));

    try(HiveLock lock = locks.getTableLock(table.getDbName(), table.getTableName())) {

      try {
        Partition existingPartition =
            client.getPartition(table.getTableName(), table.getDbName(), partition.getValues());
        partition.setCreateTime(existingPartition.getCreateTime());
        partition.setLastAccessTime(existingPartition.getLastAccessTime());
        if (needToUpdatePartition(existingPartition, partition)) {
          client.alter_partition(table.getDbName(), table.getTableName(), partition);
          log.info(String.format("Updated partition %s in table %s with location %s", partition, table.getTableName(),
              partition.getSd().getLocation()));
        } else {
          log.info(String.format("Partition %s in table %s with location %s already exists and no need to update",
              partition, table.getTableName(), partition.getSd().getLocation()));
        }
      } catch (NoSuchObjectException e) {
        client.add_partition(partition);
        log.info(String.format("Added partition %s to table %s with location %s", partition, table.getTableName(),
            partition.getSd().getLocation()));
      }

    } catch (AlreadyExistsException e) {
      // Partition already exists. Nothing to do.
    } catch (TException e) {
      log.error(String.format("Unable to add partition %s to table %s with location %s", partition,
          table.getTableName(), partition.getSd().getLocation()), e);
      throw e;
    }
  }

  private boolean needToUpdatePartition(Partition existingPartition, Partition newPartition) {
    return !existingPartition.equals(newPartition);
  }

  @Override
  public Optional<HiveTable> getTable(String dbName, String tableName) throws IOException {
    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(dbName, tableName)) {
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

    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(dbName, tableName)) {
      return Optional.of(HiveMetaStoreUtils.getHivePartition(client.get().getPartition(tableName, dbName,
          partitionValues)));
    } catch (NoSuchObjectException e) {
      return Optional.<HivePartition> absent();
    } catch (TException e) {
      throw new IOException(
          "Unable to get partition " + partitionValues + " from table " + tableName + " in db " + dbName, e);
    }
  }

  @Override
  public void alterTable(HiveTable table) throws IOException {
    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(table.getDbName(), table.getTableName())) {
      if (!client.get().tableExists(table.getDbName(), table.getTableName())) {
        throw new IOException("Table " + table.getTableName() + " in db " + table.getDbName() + " does not exist");
      }
      client.get().alter_table(table.getDbName(), table.getTableName(), HiveMetaStoreUtils.getTable(table));
    } catch (TException e) {
      throw new IOException("Unable to alter table " + table.getTableName() + " in db " + table.getDbName(), e);
    }
  }

  @Override
  public void alterPartition(HiveTable table, HivePartition partition) throws IOException {
    try(AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient();
        HiveLock lock = locks.getTableLock(table.getDbName(), table.getTableName())) {
      client.get().alter_partition(table.getDbName(), table.getTableName(), HiveMetaStoreUtils.getPartition(partition));
    } catch (TException e) {
      throw new IOException(String.format("Unable to alter partition %s in table %s in db %s", partition.getValues(),
          table.getTableName(), table.getDbName()), e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      this.clientPool.close();
    }
  }

}
