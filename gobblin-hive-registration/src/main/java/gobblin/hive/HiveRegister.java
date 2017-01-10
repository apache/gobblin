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

package gobblin.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.hive.HiveRegistrationUnit.Column;
import gobblin.hive.spec.HiveSpec;
import gobblin.hive.spec.HiveSpecWithPostActivities;
import gobblin.hive.spec.HiveSpecWithPreActivities;
import gobblin.hive.spec.HiveSpecWithPredicates;
import gobblin.hive.spec.activity.Activity;
import gobblin.util.ExecutorsUtils;
import gobblin.util.executors.ScalingThreadPoolExecutor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A class for registering Hive tables and partitions.
 *
 * @author Ziyang Liu
 */
@Slf4j
@Alpha
public abstract class HiveRegister implements Closeable {

  public static final String HIVE_REGISTER_TYPE = "hive.register.type";
  public static final String DEFAULT_HIVE_REGISTER_TYPE = "gobblin.hive.metastore.HiveMetaStoreBasedRegister";
  public static final String HIVE_TABLE_COMPARATOR_TYPE = "hive.table.comparator.type";
  public static final String DEFAULT_HIVE_TABLE_COMPARATOR_TYPE = HiveTableComparator.class.getName();
  public static final String HIVE_PARTITION_COMPARATOR_TYPE = "hive.partition.comparator.type";
  public static final String DEFAULT_HIVE_PARTITION_COMPARATOR_TYPE = HivePartitionComparator.class.getName();

  protected static final String HIVE_DB_EXTENSION = ".db";

  @Getter
  protected final HiveRegProps props;

  protected final Optional<String> hiveDbRootDir;
  protected final ListeningExecutorService executor;
  protected final List<Future<Void>> futures = Lists.newArrayList();

  protected HiveRegister(State state) {
    this.props = new HiveRegProps(state);
    this.hiveDbRootDir = this.props.getDbRootDir();
    this.executor = MoreExecutors.listeningDecorator(
        ScalingThreadPoolExecutor.newScalingThreadPool(0, this.props.getNumThreads(), TimeUnit.SECONDS.toMillis(10),
            ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of(getClass().getSimpleName()))));
  }

  /**
   * Register a table or partition given a {@link HiveSpec}. This method is asynchronous and returns immediately.
   * This methods evaluates the {@link Predicate}s and executes the {@link Activity}s specified in the
   * {@link HiveSpec}. The actual registration happens in {@link #registerPath(HiveSpec)}, which subclasses
   * should implement.
   *
   * @return a {@link ListenableFuture} for the process of registering the given {@link HiveSpec}.
   */
  public ListenableFuture<Void> register(final HiveSpec spec) {
    ListenableFuture<Void> future = this.executor.submit(new Callable<Void>() {

      @Override
      public Void call() throws Exception {

        if (spec instanceof HiveSpecWithPredicates && !evaluatePredicates((HiveSpecWithPredicates) spec)) {
          log.info("Skipping " + spec + " since predicates return false");
          return null;
        }

        if (spec instanceof HiveSpecWithPreActivities) {
          for (Activity activity : ((HiveSpecWithPreActivities) spec).getPreActivities()) {
            activity.execute(HiveRegister.this);
          }
        }

        registerPath(spec);

        if (spec instanceof HiveSpecWithPostActivities) {
          for (Activity activity : ((HiveSpecWithPostActivities) spec).getPostActivities()) {
            activity.execute(HiveRegister.this);
          }
        }

        return null;
      }

    });
    this.futures.add(future);
    return future;
  }

  private boolean evaluatePredicates(HiveSpecWithPredicates spec) {
    for (Predicate<HiveRegister> pred : spec.getPredicates()) {
      if (!pred.apply(this)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Register the path specified in the given {@link HiveSpec}.
   *
   * <p>
   *   This method should not evaluate {@link Predicate}s or execute {@link Activity}s associated with
   *   the {@link HiveSpec}, since these are done in {@link #register(HiveSpec)}.
   * </p>
   */
  protected abstract void registerPath(HiveSpec spec) throws IOException;

  /**
   * Create a Hive database if not exists.
   *
   * @param dbName the name of the database to be created.
   * @return true if the db is successfully created; false if the db already exists.
   * @throws IOException
   */
  public abstract boolean createDbIfNotExists(String dbName) throws IOException;

  /**
   * Create a Hive table if not exists.
   *
   * @param table a {@link HiveTable} to be created.
   * @return true if the table is successfully created; false if the table already exists.
   * @throws IOException
   */
  public abstract boolean createTableIfNotExists(HiveTable table) throws IOException;

  /**
   * Add a Hive partition to a table if not exists.
   *
   * @param table the {@link HiveTable} to which the partition should be added.
   * @param partition a {@link HivePartition} to be added.
   * @return true if the partition is successfully added; false if the partition already exists.
   * @throws IOException
   */
  public abstract boolean addPartitionIfNotExists(HiveTable table, HivePartition partition) throws IOException;

  /**
   * Determines whether a Hive table exists.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @return true if the table exists, false otherwise.
   * @throws IOException
   */
  public abstract boolean existsTable(String dbName, String tableName) throws IOException;

  /**
   * Determines whether a Hive partition exists.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param partitionKeys a list of {@link Columns} representing the key of the partition
   * @param partitionValues a list of Strings representing the value of the partition
   * @return true if the partition exists, false otherwise.
   * @throws IOException
   */
  public abstract boolean existsPartition(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException;

  /**
   * Drop a table if exists.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @throws IOException
   */
  public abstract void dropTableIfExists(String dbName, String tableName) throws IOException;

  /**
   * Drop a partition if exists.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param partitionKeys a list of {@link Columns} representing the key of the partition
   * @param partitionValues a list of Strings representing the value of the partition
   * @throws IOException
   */
  public abstract void dropPartitionIfExists(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException;

  /**
   * Get a {@link HiveTable} using the given db name and table name.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @return an {@link Optional} of {@link HiveTable} if the table exists, otherwise {@link Optional#absent()}.
   * @throws IOException
   */
  public abstract Optional<HiveTable> getTable(String dbName, String tableName) throws IOException;

  /**
   * Get a {@link HivePartition} using the given db name, table name, partition keys and partition values.
   *
   * @param dbName the database name
   * @param tableName the table name
   * @param partitionKeys a list of {@link Columns} representing the key of the partition
   * @param partitionValues a list of Strings representing the value of the partition
   * @return an {@link Optional} of {@link HivePartition} if the partition exists, otherwise {@link Optional#absent()}.
   * @throws IOException
   */
  public abstract Optional<HivePartition> getPartition(String dbName, String tableName, List<Column> partitionKeys,
      List<String> partitionValues) throws IOException;

  /**
   * Alter the given {@link HiveTable}. An Exception should be thrown if the table does not exist.
   *
   * @param table a {@link HiveTable} to which the existing table should be updated.
   * @throws IOException
   */
  public abstract void alterTable(HiveTable table) throws IOException;

  /**
   * Alter the given {@link HivePartition}. An Exception should be thrown if the partition does not exist.
   *
   * @param table the {@link HiveTable} to which the partition belongs.
   * @param partition a {@link HivePartition} to which the existing partition should be updated.
   * @throws IOException
   */
  public abstract void alterPartition(HiveTable table, HivePartition partition) throws IOException;

  /**
   * Create a table if not exists, or alter a table if exists.
   *
   * @param table a {@link HiveTable} to be created or altered
   * @throws IOException
   */
  public void createOrAlterTable(HiveTable table) throws IOException {
    if (!createTableIfNotExists(table)) {
      alterTable(table);
    }
  }

  /**
   * Add a partition to a table if not exists, or alter a partition if exists.
   *
   * @param table the {@link HiveTable} to which the partition belongs.
   * @param partition a {@link HivePartition} to which the existing partition should be updated.
   * @throws IOException
   */
  public void addOrAlterPartition(HiveTable table, HivePartition partition) throws IOException {
    if (!addPartitionIfNotExists(table, partition)) {
      alterPartition(table, partition);
    }
  }

  protected HiveRegistrationUnitComparator<?> getTableComparator(HiveTable existingTable, HiveTable newTable) {
    try {
      Class<?> clazz =
          Class.forName(this.props.getProp(HIVE_TABLE_COMPARATOR_TYPE, DEFAULT_HIVE_TABLE_COMPARATOR_TYPE));
      return (HiveRegistrationUnitComparator<?>) ConstructorUtils.invokeConstructor(clazz, existingTable, newTable);
    } catch (ReflectiveOperationException e) {
      log.error("Unable to instantiate Hive table comparator", e);
      throw Throwables.propagate(e);
    }
  }

  protected boolean needToUpdateTable(HiveTable existingTable, HiveTable newTable) {
    return getTableComparator(existingTable, newTable).compareAll().result();
  }

  protected HiveRegistrationUnitComparator<?> getPartitionComparator(HivePartition existingPartition,
      HivePartition newPartition) {
    try {
      Class<?> clazz =
          Class.forName(this.props.getProp(HIVE_PARTITION_COMPARATOR_TYPE, DEFAULT_HIVE_PARTITION_COMPARATOR_TYPE));
      return (HiveRegistrationUnitComparator<?>) ConstructorUtils.invokeConstructor(clazz, existingPartition,
          newPartition);
    } catch (ReflectiveOperationException e) {
      log.error("Unable to instantiate Hive partition comparator", e);
      throw Throwables.propagate(e);
    }
  }

  protected boolean needToUpdatePartition(HivePartition existingPartition, HivePartition newPartition) {
    return getPartitionComparator(existingPartition, newPartition).compareAll().result();
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
      ExecutorsUtils.shutdownExecutorService(this.executor, Optional.of(log));
    }
  }

  /**
   * Get an instance of {@link HiveRegister}.
   *
   * @param props A {@link State} object. To get a specific implementation of {@link HiveRegister},
   * specify property {@link #HIVE_REGISTER_TYPE} as the class name. Otherwise, {@link #DEFAULT_HIVE_REGISTER_TYPE}
   * will be returned. This {@link State} object is also used to instantiate the {@link HiveRegister} object.
   */
  public static HiveRegister get(State props) {
    return get(props, Optional.<String> absent());
  }

  /**
   * Get an instance of {@link HiveRegister}.
   *
   * @param props A {@link State} object. To get a specific implementation of {@link HiveRegister},
   * specify property {@link #HIVE_REGISTER_TYPE} as the class name. Otherwise, {@link #DEFAULT_HIVE_REGISTER_TYPE}
   * will be returned. This {@link State} object is also used to instantiate the {@link HiveRegister} object.
   */
  public static HiveRegister get(State props, Optional<String> metastoreURI) {
    return get(props.getProp(HIVE_REGISTER_TYPE, DEFAULT_HIVE_REGISTER_TYPE), props, metastoreURI);
  }

  /**
   * Get an instance of {@link HiveRegister}.
   *
   * @param hiveRegisterType The name of a class that implements {@link HiveRegister}.
   * @param props A {@link State} object used to instantiate the {@link HiveRegister} object.
   */
  public static HiveRegister get(String hiveRegisterType, State props, Optional<String> metastoreURI) {
    try {
      return (HiveRegister) ConstructorUtils.invokeConstructor(Class.forName(hiveRegisterType), props, metastoreURI);
    } catch (ReflectiveOperationException e) {
      throw Throwables.propagate(e);
    }
  }

}
