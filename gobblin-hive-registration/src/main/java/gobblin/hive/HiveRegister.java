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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Striped;

import gobblin.configuration.State;
import gobblin.util.ExecutorsUtils;
import lombok.extern.slf4j.Slf4j;


/**
 * A class for registering Hive tables or partitions.
 *
 * An instance of this class is initialized with a {@link State} object, a {@link HiveRegistrationPolicy} and a
 * {@link HiveStorageDescriptorManager}. The {@link #register(HiveRegistrable)} method is asynchronous and
 * returns immediately.
 *
 * @author ziliu
 */
@Slf4j
public class HiveRegister implements Closeable {

  public static final String HIVE_DB_ROOT_DIR = "hive.db.root.dir";
  public static final String HIVE_OWNER = "hive.owner";
  public static final String HIVE_REGISTER_THREADS = "hive.register.threads";
  public static final int DEFAULT_HIVE_REGISTER_THREADS = 20;

  private static final String HIVE_DB_EXTENSION = ".db";

  private final State state;
  private final String hiveDbRootDir;
  private final String hiveOwner;
  private final HiveRegistrationPolicy partitionManager;
  private final HiveStorageDescriptorManager sdManager;
  private final HiveMetaStoreClient client;

  private final ExecutorService executor;
  private final List<Future<Void>> futures = Lists.newArrayList();
  private final Striped<Lock> locks = Striped.lazyWeakLock(Integer.MAX_VALUE);

  public HiveRegister(State state, HiveRegistrationPolicy partitionManager, HiveStorageDescriptorManager sdManager)
      throws MetaException {
    Preconditions.checkArgument(state.contains(HIVE_DB_ROOT_DIR), "Missing required property " + HIVE_DB_ROOT_DIR);
    Preconditions.checkArgument(state.contains(HIVE_OWNER), "Missing required property " + HIVE_OWNER);

    this.state = state;
    this.hiveDbRootDir = this.state.getProp(HIVE_DB_ROOT_DIR);
    this.hiveOwner = this.state.getProp(HIVE_OWNER);
    this.partitionManager = partitionManager;
    this.sdManager = sdManager;
    this.client = new HiveMetaStoreClient(new HiveConf());
    this.executor =
        Executors.newFixedThreadPool(this.state.getPropAsInt(HIVE_REGISTER_THREADS, DEFAULT_HIVE_REGISTER_THREADS),
            ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("HiveRegister")));
  }

  /**
   * Register a {@link HiveRegistrable}, which can be a table or a partition.
   *
   * This method is asynchronous and returns immediately.
   */
  public void register(final HiveRegistrable registrable) throws IOException {
    this.futures.add(this.executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {
        String dbName = HiveRegister.this.partitionManager.getDatabaseName(registrable);
        String tableName = HiveRegister.this.partitionManager.getTableName(registrable);

        if (existsAny(dbName, HiveRegister.this.partitionManager.getDominatingTableNames(registrable))
            || existsAny(dbName, tableName, HiveRegister.this.partitionManager.getDominatingPartitions(registrable))) {
          return null;
        }

        createDbIfNotExists(dbName);

        Optional<HivePartition> partition = HiveRegister.this.partitionManager.getPartition(registrable);

        Table table = getOrCreateTable(dbName, tableName, partition, HiveRegister.this.sdManager
            .getStorageDescriptor(registrable, HiveRegister.this.partitionManager.getTableLocataion(registrable)));

        if (table.getPartitionKeys().isEmpty()) {

          // Register a non-partitioned table
          HiveRegister.this.client.alter_table(dbName, tableName, table);
        } else {

          // Register a partition
          addPartition(table, partition.get(), HiveRegister.this.sdManager.getStorageDescriptor(registrable));
        }

        dropObsoleteTables(dbName, HiveRegister.this.partitionManager.getObsoleteTableNames(registrable));
        dropObsoletePartitions(dbName, tableName,
            HiveRegister.this.partitionManager.getObsoletePartitions(registrable));

        return null;
      }
    }));

  }

  private void createDbIfNotExists(String dbName) {
    Database db = new Database();
    db.setName(dbName);
    db.setLocationUri(this.hiveDbRootDir + Path.SEPARATOR + dbName + HIVE_DB_EXTENSION);

    Lock lock = locks.get(dbName);
    lock.lock();

    try {
      client.createDatabase(db);
    } catch (AlreadyExistsException e) {
      log.warn("Hive database " + dbName + " already exists");
    } catch (TException e) {
      log.error("Unable to create Hive database " + dbName);
      throw Throwables.propagate(e);
    } finally {
      lock.unlock();
    }
  }

  private Table getOrCreateTable(String dbName, String tableName, Optional<HivePartition> partition,
      StorageDescriptor sd) {
    Lock lock = locks.get(dbName + tableName);
    lock.lock();

    try {
      return this.client.getTable(dbName, tableName);
    } catch (NoSuchObjectException e) {
      return createTable(dbName, tableName, partition, sd);
    } catch (TException e) {
      log.error(String.format("Unable to check existence of Hive table %s in db %s", tableName, dbName));
      throw Throwables.propagate(e);
    } finally {
      lock.unlock();
    }
  }

  private Table createTable(String dbName, String tableName, Optional<HivePartition> partition, StorageDescriptor sd) {
    Table table = new Table();
    table.setParameters(Maps.<String, String> newHashMap());
    table.getParameters().put("EXTERNAL", Boolean.TRUE.toString());
    table.setDbName(dbName);
    table.setTableName(tableName);
    if (partition.isPresent()) {
      table.setPartitionKeys(partition.get().getKeys());
    }
    table.setOwner(this.hiveOwner);
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(sd);
    return table;
  }

  /**
   * Whether any of the given tables exists.
   */
  private boolean existsAny(String dbName, Collection<String> tableNames) {
    for (String tableName : tableNames) {
      try {
        if (this.client.tableExists(dbName, tableName)) {
          return true;
        }
      } catch (TException e) {
        log.error(String.format("Unable to check existence of Hive table %s in db %s", tableName, dbName));
        throw Throwables.propagate(e);
      }
    }
    return false;
  }

  /**
   * Whether any of the given partitions exists.
   */
  private boolean existsAny(String dbName, String tableName, Collection<HivePartition> partitions) {
    for (HivePartition partition : partitions) {
      try {
        client.getPartition(dbName, tableName, partition.getValues());
        return true;
      } catch (NoSuchObjectException e) {
        // Partition does not exist. Nothing to do
      } catch (TException e) {
        log.error(String.format("Unable to check existence of Hive partition %s in table %s db %s", partition,
            tableName, dbName));
        throw Throwables.propagate(e);
      }
    }
    return false;
  }

  private void dropObsoleteTables(String dbName, Collection<String> tableNames) throws TException {
    for (String tableName : tableNames) {
      Lock lock = locks.get(dbName + tableName);
      lock.lock();

      try {
        if (this.client.tableExists(dbName, tableName)) {
          this.client.dropTable(dbName, tableName);
        }
      } finally {
        lock.unlock();
      }
    }
  }

  private void dropObsoletePartitions(String dbName, String tableName, Collection<HivePartition> partitions)
      throws TException {
    Lock lock = locks.get(dbName + tableName);
    lock.lock();

    try {
      for (HivePartition partition : partitions) {
        try {
          client.dropPartition(dbName, tableName, partition.getValues(), false);
        } catch (NoSuchObjectException e) {
          // Partition does not exist. Nothing to do
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private void addPartition(Table table, HivePartition partition, StorageDescriptor sd) throws TException {
    Preconditions.checkArgument(table.getPartitionKeysSize() == partition.getValues().size());

    Partition p = new Partition();
    p.setDbName(table.getDbName());
    p.setTableName(table.getTableName());
    p.setValues(partition.getValues());
    p.setSd(sd);
    log.info(String.format("Adding partition %s to table %s with location %s", partition, table.getTableName(),
        p.getSd().getLocation()));

    Lock lock = locks.get(table.getDbName() + table.getTableName());
    lock.lock();

    try {
      this.client.add_partition(p);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Wait till all registration requested submitted via {@link #register(HiveRegistrable)} to finish.
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
      ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(log));
    }
  }
}
