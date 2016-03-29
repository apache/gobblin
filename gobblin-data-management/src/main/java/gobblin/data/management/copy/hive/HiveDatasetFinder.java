/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import gobblin.dataset.IterableDatasetFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HiveRegProps;
import gobblin.util.AutoReturnableObject;

import javax.annotation.Nullable;


/**
 * Finds {@link HiveDataset}s. Will look for tables in a database specified by {@link #DB_PATTERN_KEY}, possibly filtering them
 * with pattern {@link #TABLE_PATTERN_KEY}, and create a {@link HiveDataset} for each one.
 */
public class HiveDatasetFinder implements IterableDatasetFinder<HiveDataset> {

  public static final String HIVE_DATASET_PREFIX = "hive.dataset";
  public static final String HIVE_METASTORE_URI_KEY = HIVE_DATASET_PREFIX + ".hive.metastore.uri";
  public static final String DB_PATTERN_KEY = HIVE_DATASET_PREFIX + ".database.pattern";
  public static final String TABLE_PATTERN_KEY = HIVE_DATASET_PREFIX + ".table.pattern";
  public static final String DEFAULT_TABLE_PATTERN = "*";

  private final HiveRegProps hiveProps;
  private final Properties properties;
  private final HiveMetastoreClientPool clientPool;
  private final FileSystem fs;
  private final String dbPattern;
  private final String tablePattern;

  public HiveDatasetFinder(FileSystem fs, Properties properties) throws IOException {

    Preconditions.checkArgument(properties.containsKey(DB_PATTERN_KEY));

    this.fs = fs;
    this.clientPool = HiveMetastoreClientPool.get(properties,
        Optional.fromNullable(properties.getProperty(HIVE_METASTORE_URI_KEY)));
    this.hiveProps = this.clientPool.getHiveRegProps();

    this.dbPattern = properties.getProperty(DB_PATTERN_KEY);
    this.tablePattern = properties.getProperty(TABLE_PATTERN_KEY, DEFAULT_TABLE_PATTERN);
    this.properties = properties;
  }

  /**
   * Get all tables in db with given table pattern.
   */
  public Iterator<Table> getTables() throws IOException {
    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      List<String> dbNames = client.get().getDatabases(this.dbPattern);
      return new TableIterator(dbNames);
    } catch (Exception exc) {
      throw new IOException(exc);
    }
  }

  private class TableIterator implements Iterator<Table> {

    private final Iterator<String> databases;

    private Iterator<String> tablesInDb;
    private String currentDb;

    public TableIterator(List<String> databases) {
      this.databases = databases.iterator();
    }

    @Override
    public boolean hasNext() {
      return (this.tablesInDb.hasNext() || this.databases.hasNext());
    }

    @Override
    public Table next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      try (AutoReturnableObject<IMetaStoreClient> client = clientPool.getClient()) {
        if (!this.tablesInDb.hasNext()) {
          this.currentDb = this.databases.next();
          this.tablesInDb = client.get().getTables(this.currentDb, tablePattern).iterator();
        }

        return client.get().getTable(this.currentDb, this.tablesInDb.next());
      } catch (IOException | TException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  @Override public List<HiveDataset> findDatasets() throws IOException {
    return Lists.newArrayList(getDatasetsIterator());
  }

  @Override
  public Iterator<HiveDataset> getDatasetsIterator()
      throws IOException {
    return Iterators.transform(getTables(), new Function<Table, HiveDataset>() {
      @Nullable
      @Override
      public HiveDataset apply(@Nullable Table table) {
        try {
          return new HiveDataset(fs, clientPool, new org.apache.hadoop.hive.ql.metadata.Table(table), properties);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    });
  }

  @Override public Path commonDatasetRoot() {
    return new Path("/");
  }
}
