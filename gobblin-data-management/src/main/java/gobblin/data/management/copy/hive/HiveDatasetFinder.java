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
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.dataset.IterableDatasetFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventKeys;
import gobblin.util.AutoReturnableObject;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;


/**
 * Finds {@link HiveDataset}s. Will look for tables in a database using a {@link WhitelistBlacklist},
 * and creates a {@link HiveDataset} for each one.
 */
@Slf4j
public class HiveDatasetFinder implements IterableDatasetFinder<HiveDataset> {

  public static final String HIVE_DATASET_PREFIX = "hive.dataset";
  public static final String HIVE_METASTORE_URI_KEY = HIVE_DATASET_PREFIX + ".hive.metastore.uri";
  public static final String DB_KEY = HIVE_DATASET_PREFIX + ".database";
  public static final String TABLE_PATTERN_KEY = HIVE_DATASET_PREFIX + ".table.pattern";
  public static final String DEFAULT_TABLE_PATTERN = "*";

  // Event names
  private static final String DATASET_FOUND = "DatasetFound";
  private static final String DATASET_ERROR = "DatasetError";
  private static final String FAILURE_CONTEXT = "FailureContext";

  private final Properties properties;
  private final HiveMetastoreClientPool clientPool;
  private final FileSystem fs;
  private final WhitelistBlacklist whitelistBlacklist;
  private final Optional<EventSubmitter> eventSubmitter;

  public HiveDatasetFinder(FileSystem fs, Properties properties) throws IOException {
    this(fs, properties, createClientPool(properties));
  }

  public HiveDatasetFinder(FileSystem fs, Properties properties, EventSubmitter eventSubmitter) throws IOException {
    this(fs, properties, createClientPool(properties), eventSubmitter);
  }

  protected HiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool clientPool)
      throws IOException {
    this(fs, properties, clientPool, null);
  }

  protected HiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool clientPool,
      EventSubmitter eventSubmitter) throws IOException {
    this.properties = properties;
    this.clientPool = clientPool;
    this.fs = fs;

    String whitelistKey = HIVE_DATASET_PREFIX + "." + WhitelistBlacklist.WHITELIST;
    Preconditions.checkArgument(properties.containsKey(DB_KEY) || properties.containsKey(whitelistKey),
        String.format("Must specify %s or %s.", DB_KEY, whitelistKey));

    Config config = ConfigFactory.parseProperties(properties);

    if (properties.containsKey(DB_KEY)) {
      this.whitelistBlacklist = new WhitelistBlacklist(this.properties.getProperty(DB_KEY) + "."
          + this.properties.getProperty(TABLE_PATTERN_KEY, DEFAULT_TABLE_PATTERN), "");
    } else {
      this.whitelistBlacklist = new WhitelistBlacklist(config.getConfig(HIVE_DATASET_PREFIX));
    }

    this.eventSubmitter = Optional.fromNullable(eventSubmitter);
  }

  protected static HiveMetastoreClientPool createClientPool(Properties properties) throws IOException {
    return HiveMetastoreClientPool.get(properties,
        Optional.fromNullable(properties.getProperty(HIVE_METASTORE_URI_KEY)));
  }

  /**
   * Get all tables in db with given table pattern.
   */
  public Collection<DbAndTable> getTables() throws IOException {
    List<DbAndTable> tables = Lists.newArrayList();

    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      Iterable<String> databases = Iterables.filter(client.get().getAllDatabases(), new Predicate<String>() {
        @Override
        public boolean apply(String db) {
          return HiveDatasetFinder.this.whitelistBlacklist.acceptDb(db);
        }
      });
      for (final String db : databases) {

        Iterable<String> tableNames = Iterables.filter(client.get().getAllTables(db), new Predicate<String>() {
          @Override
          public boolean apply(String table) {
            return HiveDatasetFinder.this.whitelistBlacklist.acceptTable(db, table);
          }
        });
        for (String tableName : tableNames) {
          tables.add(new DbAndTable(db, tableName));
        }
      }
    } catch (Exception exc) {
      throw new IOException(exc);
    }

    return tables;
  }

  @Data
  public static class DbAndTable {
    private final String db;
    private final String table;

    @Override
    public String toString() {
      return String.format("%s.%s", this.db, this.table);
    }
  }

  @Override
  public List<HiveDataset> findDatasets() throws IOException {
    return Lists.newArrayList(getDatasetsIterator());
  }

  @Override
  public Iterator<HiveDataset> getDatasetsIterator() throws IOException {

    return new AbstractIterator<HiveDataset>() {
      private Iterator<DbAndTable> tables = getTables().iterator();

      @Override
      protected HiveDataset computeNext() {
        while (this.tables.hasNext()) {
          DbAndTable dbAndTable = this.tables.next();
          try (AutoReturnableObject<IMetaStoreClient> client = HiveDatasetFinder.this.clientPool.getClient()) {
            Table table = client.get().getTable(dbAndTable.getDb(), dbAndTable.getTable());
            EventSubmitter.submit(HiveDatasetFinder.this.eventSubmitter, DATASET_FOUND, SlaEventKeys.DATASET_URN_KEY, dbAndTable.toString());
            return createHiveDataset(table);
          } catch (Throwable t) {
            log.error(String.format("Failed to create HiveDataset for table %s.%s", dbAndTable.getDb(), dbAndTable.getTable()), t);
            EventSubmitter.submit(HiveDatasetFinder.this.eventSubmitter, DATASET_ERROR,
                SlaEventKeys.DATASET_URN_KEY, dbAndTable.toString(),
                FAILURE_CONTEXT, t.toString());
          }
        }
        return endOfData();
      }
    };
  }

  protected HiveDataset createHiveDataset(Table table) throws IOException {
    return new HiveDataset(this.fs, this.clientPool, new org.apache.hadoop.hive.ql.metadata.Table(table),
        this.properties);
  }

  @Override
  public Path commonDatasetRoot() {
    return new Path("/");
  }
}
