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

package gobblin.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HiveStripedLocks.HiveLock;
import gobblin.hive.policy.HiveRegistrationPolicy;
import gobblin.hive.policy.HiveRegistrationPolicyBase;
import gobblin.hive.spec.HiveSpec;
import gobblin.hive.spec.HiveSpecWithPostActivities;
import gobblin.hive.spec.HiveSpecWithPreActivities;
import gobblin.hive.spec.HiveSpecWithPredicates;
import gobblin.hive.spec.activity.Activity;
import gobblin.util.ExecutorsUtils;
import gobblin.util.executors.ScalingThreadPoolExecutor;
import lombok.extern.slf4j.Slf4j;


/**
 * A class for registering Hive tables or partitions.
 *
 * <p>
 *   An instance of this class is initialized with a {@link State} object. To register a table or partition,
 *   properties {@link HiveRegProps#HIVE_DB_ROOT_DIR}, {@link HiveRegProps#HIVE_OWNER} are required. They are
 *   optional for de-registering or checking the existence of a table or partition.
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
public class HiveRegister implements Closeable {

  private static final String HIVE_DB_EXTENSION = ".db";

  private final HiveRegProps props;
  private final Optional<String> hiveDbRootDir;
  private final Optional<String> hiveOwner;
  private final GenericObjectPool<IMetaStoreClient> clientPool;

  private final ListeningExecutorService executor;
  private final List<Future<Void>> futures = Lists.newArrayList();
  private final HiveStripedLocks locks = new HiveStripedLocks();

  public HiveRegister(State state) throws IOException {
    this.props = new HiveRegProps(state);
    this.hiveDbRootDir = this.props.getDbRootDir();
    this.hiveOwner = this.props.getHiveOwner();

    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(this.props.getNumThreads());
    config.setMaxIdle(this.props.getNumThreads());
    this.clientPool = new GenericObjectPool<>(new HiveMetaStoreClientFactory(), config);

    this.executor = MoreExecutors.listeningDecorator(
        ScalingThreadPoolExecutor.newScalingThreadPool(0, this.props.getNumThreads(), TimeUnit.SECONDS.toMillis(10),
            ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of(getClass().getSimpleName()))));
  }

  /**
   * Register a table or partition given a {@link HiveSpec}.
   *
   * This method is asynchronous and returns immediately.
   *
   * @return a {@link ListenableFuture} for the process of registering the given {@link HiveSpec}.
   */
  public ListenableFuture<Void> register(final HiveSpec spec) throws IOException {
    ListenableFuture<Void> future = this.executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {

        IMetaStoreClient client = borrowClient();

        try {

          if (spec instanceof HiveSpecWithPredicates && !evaluatePredicates((HiveSpecWithPredicates) spec)) {
            log.info("Skipping " + spec + " since predicates return false");
            return null;
          }

          if (spec instanceof HiveSpecWithPreActivities) {
            for (Activity activity : ((HiveSpecWithPreActivities) spec).getActivities()) {
              activity.execute(HiveRegister.this);
            }
          }

          String dbName = spec.getDbName();
          String tableName = spec.getTableName();

          createDbIfNotExists(client, dbName);
          Optional<HivePartition> partition = spec.getPartition();
          Table table = createOrAlterTable(client, dbName, tableName, partition, spec.getSd());

          if (!table.getPartitionKeys().isEmpty()) {

            // Register a partition
            addOrAlterPartition(client, table, partition.get(), spec.getPath());
          }

          if (spec instanceof HiveSpecWithPostActivities) {
            for (Activity activity : ((HiveSpecWithPostActivities) spec).getActivities()) {
              activity.execute(HiveRegister.this);
            }
          }

          return null;
        } finally {
          HiveRegister.this.clientPool.returnObject(client);
        }
      }
    });

    this.futures.add(future);
    return future;
  }

  /**
   * Register the given {@link Path}s.
   *
   * @param paths The {@link Path}s to be registered.
   * @param state A {@link State} which will be used to instantiate a {@link HiveRegister} and a
   * {@link HiveRegistrationPolicy} for registering the given The {@link Path}s.
   */
  public static void register(Iterable<String> paths, State state) throws IOException {
    try (HiveRegister hiveRegister = new HiveRegister(state)) {
      HiveRegistrationPolicy policy = HiveRegistrationPolicyBase.getPolicy(state);
      for (String path : paths) {
        hiveRegister.register(policy.getHiveSpec(new Path(path)));
      }
    }
  }

  private boolean evaluatePredicates(HiveSpecWithPredicates spec) {
    for (Predicate<HiveRegister> pred : spec.getPredicates()) {
      if (!pred.apply(this)) {
        return false;
      }
    }
    return true;
  }

  private void createDbIfNotExists(IMetaStoreClient client, String dbName) throws IOException {
    Database db = new Database();
    db.setName(dbName);

    Preconditions.checkState(this.hiveDbRootDir.isPresent(),
        "Missing required property " + HiveRegProps.HIVE_DB_ROOT_DIR);
    db.setLocationUri(new Path(this.hiveDbRootDir.get(), dbName + HIVE_DB_EXTENSION).toString());

    HiveLock lock = locks.getDbLock(dbName);
    lock.lock();

    try {
      client.createDatabase(db);
      log.info("Created database " + dbName);
    } catch (AlreadyExistsException e) {
      // Database already exists. Nothing to do.
    } catch (TException e) {
      throw new IOException("Unable to create Hive database " + dbName, e);
    } finally {
      lock.unlock();
    }
  }

  private Table createOrAlterTable(IMetaStoreClient client, String dbName, String tableName,
      Optional<HivePartition> partition, StorageDescriptor sd) throws IOException {

    Table newTable = createTableWithoutRegistering(dbName, tableName, partition, sd);

    HiveLock lock = locks.getTableLock(dbName, tableName);
    lock.lock();

    try {
      if (client.tableExists(dbName, tableName)) {
        Table existingTable = client.getTable(dbName, tableName);
        newTable.setCreateTime(existingTable.getCreateTime());
        newTable.setLastAccessTime(existingTable.getLastAccessTime());
        if (needToUpdateTable(existingTable, newTable)) {
          client.alter_table(dbName, tableName, newTable);
          log.info(String.format("updated Hive table %s in db %s", tableName, dbName));
          return newTable;
        } else {
          log.info(String.format("Hive table %s in db %s exists and no need to update", tableName, dbName));
          return existingTable;
        }
      } else {
        client.createTable(newTable);
        log.info(String.format("Created Hive table %s in db %s", tableName, dbName));
        return newTable;
      }
    } catch (TException e) {
      throw new IOException(String.format("Error in creating or altering Hive table %s in db %s", tableName, dbName),
          e);
    } finally {
      lock.unlock();
    }
  }

  private Table createTableWithoutRegistering(String dbName, String tableName, Optional<HivePartition> partition,
      StorageDescriptor sd) {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("EXTERNAL", Boolean.TRUE.toString());
    Table table = new Table();
    table.setParameters(parameters);
    table.setDbName(dbName);
    table.setTableName(tableName);

    Preconditions.checkArgument(this.hiveOwner.isPresent(), "Missing required property " + HiveRegProps.HIVE_OWNER);
    table.setOwner(this.hiveOwner.get());
    table.setTableType(TableType.EXTERNAL_TABLE.toString());

    if (partition.isPresent()) {
      table.setPartitionKeys(partition.get().getKeys());
    }

    table.setSd(sd);
    return table;
  }

  private boolean needToUpdateTable(Table existingTable, Table newTable) {
    return !existingTable.equals(newTable);
  }

  public boolean existsTable(String dbName, String tableName) throws IOException {
    IMetaStoreClient client = borrowClient();
    HiveLock lock = locks.getTableLock(dbName, tableName);
    lock.lock();

    try {
      return client.tableExists(dbName, tableName);
    } catch (TException e) {
      throw new IOException(String.format("Unable to check existence of table %s in db %s", tableName, dbName), e);
    } finally {
      lock.unlock();
      this.clientPool.returnObject(client);
    }
  }

  public boolean existsPartition(String dbName, String tableName, List<String> partitionValues) throws IOException {
    IMetaStoreClient client = borrowClient();
    HiveLock lock = locks.getTableLock(dbName, tableName);
    lock.lock();

    try {
      client.getPartition(dbName, tableName, partitionValues);
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      throw new IOException(String.format("Unable to check existence of partition %s in table %s in db %s",
          partitionValues, tableName, dbName), e);
    } finally {
      lock.unlock();
      this.clientPool.returnObject(client);
    }
  }

  public void dropTableIfExists(String dbName, String tableName) throws IOException {
    IMetaStoreClient client = borrowClient();
    HiveLock lock = locks.getTableLock(dbName, tableName);
    lock.lock();

    try {
      if (client.tableExists(dbName, tableName)) {
        client.dropTable(dbName, tableName);
        log.info("Dropped table " + tableName + " in db " + dbName);
      }
    } catch (TException e) {
      throw new IOException(String.format("Unable to deregister table %s in db %s", tableName, dbName), e);
    } finally {
      lock.unlock();
      this.clientPool.returnObject(client);
    }
  }

  public void dropPartitionIfExists(String dbName, String tableName, List<String> partitionValues) throws IOException {
    IMetaStoreClient client = borrowClient();
    HiveLock lock = locks.getTableLock(dbName, tableName);
    lock.lock();

    try {
      client.dropPartition(dbName, tableName, partitionValues, false);
      log.info("Dropped partition " + partitionValues + " in table " + tableName + " in db " + dbName);
    } catch (NoSuchObjectException e) {
      // Partition does not exist. Nothing to do
    } catch (TException e) {
      throw new IOException(String.format("Unable to check existence of Hive partition %s in table %s in db %s",
          partitionValues, tableName, dbName), e);
    } finally {
      lock.unlock();
      this.clientPool.returnObject(client);
    }
  }

  private IMetaStoreClient borrowClient() throws IOException {
    try {
      return this.clientPool.borrowObject();
    } catch (Exception e) {
      throw new IOException("Unable to borrow " + IMetaStoreClient.class.getSimpleName());
    }
  }

  private void addOrAlterPartition(IMetaStoreClient client, Table table, HivePartition partition,
      Path partitionLocation) throws TException {
    Preconditions.checkArgument(table.getPartitionKeysSize() == partition.getValues().size());

    Partition newPartition = createNewPartitionWithoutRegistering(table, partition, partitionLocation);

    HiveLock lock = locks.getTableLock(table.getDbName(), table.getTableName());
    lock.lock();

    try {

      try {
        Partition existingPartition =
            client.getPartition(table.getDbName(), table.getTableName(), partition.getValues());
        if (needToUpdatePartition(existingPartition, newPartition)) {
          client.alter_partition(table.getDbName(), table.getTableName(), newPartition);
          log.info(String.format("Updated partition %s in table %s with location %s", partition, table.getTableName(),
              newPartition.getSd().getLocation()));
        } else {
          log.info(String.format("Partition %s in table %s with location %s already exists and no need to update",
              partition, table.getTableName(), newPartition.getSd().getLocation()));
        }
      } catch (NoSuchObjectException e) {
        client.add_partition(newPartition);
        log.info(String.format("Added partition %s to table %s with location %s", partition, table.getTableName(),
            newPartition.getSd().getLocation()));
      }

    } catch (AlreadyExistsException e) {
      // Partition already exists. Nothing to do.
    } catch (TException e) {
      log.error(String.format("Unable to add partition %s to table %s with location %s", partition,
          table.getTableName(), newPartition.getSd().getLocation()), e);
      throw e;
    } finally {
      lock.unlock();
    }
  }

  private Partition createNewPartitionWithoutRegistering(Table table, HivePartition partition, Path partitionLocation) {
    Partition newPartition = new Partition();
    newPartition.setDbName(table.getDbName());
    newPartition.setTableName(table.getTableName());
    newPartition.setValues(partition.getValues());
    newPartition.setSd(table.getSd());
    newPartition.getSd().setLocation(partitionLocation.toString());
    return newPartition;
  }

  private boolean needToUpdatePartition(Partition existingPartition, Partition newPartition) {
    return !existingPartition.equals(newPartition);
  }

  /**
   * Wait till all registration requested submitted via {@link #register(HiveSpec)} to finish.
   *
   * @throws IOException if any registration failed or was interrupted.
   */
  @Override
  public void close() throws IOException {
    try {
      for (Future<Void> future : this.futures) {
        future.get();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      try {
        this.clientPool.close();
      } finally {
        ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(log));
      }
    }
  }
}
