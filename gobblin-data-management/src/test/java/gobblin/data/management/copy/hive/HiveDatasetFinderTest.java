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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;

import gobblin.hive.HiveMetastoreClientPool;
import gobblin.util.AutoReturnableObject;


public class HiveDatasetFinderTest {

  @Test
  public void testDatasetFinder() throws Exception {

    List<HiveDatasetFinder.DbAndTable> dbAndTables = Lists.newArrayList();
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table1"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table2"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table3"));
    HiveMetastoreClientPool pool = getTestPool(dbAndTables);

    Properties properties = new Properties();
    properties.put(HiveDatasetFinder.HIVE_DATASET_PREFIX + "." + WhitelistBlacklist.WHITELIST, "");

    HiveDatasetFinder finder = new TestHiveDatasetFinder(FileSystem.getLocal(new Configuration()), properties, pool);
    List<HiveDataset> datasets = Lists.newArrayList(finder.getDatasetsIterator());

    Assert.assertEquals(datasets.size(), 3);
  }

  @Test
  public void testWhitelist() throws Exception {

    List<HiveDatasetFinder.DbAndTable> dbAndTables = Lists.newArrayList();
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table1"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table2"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db2", "table1"));
    HiveMetastoreClientPool pool = getTestPool(dbAndTables);

    Properties properties = new Properties();
    properties.put(HiveDatasetFinder.HIVE_DATASET_PREFIX + "." + WhitelistBlacklist.WHITELIST, "db1");

    HiveDatasetFinder finder = new TestHiveDatasetFinder(FileSystem.getLocal(new Configuration()), properties, pool);
    List<HiveDataset> datasets = Lists.newArrayList(finder.getDatasetsIterator());

    Assert.assertEquals(datasets.size(), 2);
    Assert.assertEquals(datasets.get(0).getTable().getDbName(), "db1");
    Assert.assertEquals(datasets.get(0).getTable().getTableName(), "table1");
    Assert.assertEquals(datasets.get(1).getTable().getDbName(), "db1");
    Assert.assertEquals(datasets.get(1).getTable().getTableName(), "table2");
  }

  @Test
  public void testBlacklist() throws Exception {

    List<HiveDatasetFinder.DbAndTable> dbAndTables = Lists.newArrayList();
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table1"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table2"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db2", "table1"));
    HiveMetastoreClientPool pool = getTestPool(dbAndTables);

    Properties properties = new Properties();
    properties.put(HiveDatasetFinder.HIVE_DATASET_PREFIX + "." + WhitelistBlacklist.WHITELIST, "");
    properties.put(HiveDatasetFinder.HIVE_DATASET_PREFIX + "." + WhitelistBlacklist.BLACKLIST, "db2");


    HiveDatasetFinder finder = new TestHiveDatasetFinder(FileSystem.getLocal(new Configuration()), properties, pool);
    List<HiveDataset> datasets = Lists.newArrayList(finder.getDatasetsIterator());

    Assert.assertEquals(datasets.size(), 2);
    Assert.assertEquals(datasets.get(0).getTable().getDbName(), "db1");
    Assert.assertEquals(datasets.get(0).getTable().getTableName(), "table1");
    Assert.assertEquals(datasets.get(1).getTable().getDbName(), "db1");
    Assert.assertEquals(datasets.get(1).getTable().getTableName(), "table2");
  }

  @Test
  public void testTableList() throws Exception {
    List<HiveDatasetFinder.DbAndTable> dbAndTables = Lists.newArrayList();
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table1"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table2"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table3"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db2", "table1"));
    HiveMetastoreClientPool pool = getTestPool(dbAndTables);

    Properties properties = new Properties();
    properties.put(HiveDatasetFinder.DB_KEY, "db1");
    properties.put(HiveDatasetFinder.TABLE_PATTERN_KEY, "table1|table2");


    HiveDatasetFinder finder = new TestHiveDatasetFinder(FileSystem.getLocal(new Configuration()), properties, pool);
    List<HiveDataset> datasets = Lists.newArrayList(finder.getDatasetsIterator());

    Assert.assertEquals(datasets.size(), 2);
    Assert.assertEquals(datasets.get(0).getTable().getDbName(), "db1");
    Assert.assertEquals(datasets.get(0).getTable().getTableName(), "table1");
    Assert.assertEquals(datasets.get(1).getTable().getDbName(), "db1");
    Assert.assertEquals(datasets.get(1).getTable().getTableName(), "table2");
  }

  private HiveMetastoreClientPool getTestPool(List<HiveDatasetFinder.DbAndTable> dbAndTables) throws Exception {

    SetMultimap<String, String> entities = HashMultimap.create();
    for (HiveDatasetFinder.DbAndTable dbAndTable : dbAndTables) {
      entities.put(dbAndTable.getDb(), dbAndTable.getTable());
    }

    HiveMetastoreClientPool pool = Mockito.mock(HiveMetastoreClientPool.class);

    IMetaStoreClient client = Mockito.mock(IMetaStoreClient.class);
    Mockito.when(client.getAllDatabases()).thenReturn(Lists.newArrayList(entities.keySet()));
    for (String db : entities.keySet()) {
      Mockito.doReturn(Lists.newArrayList(entities.get(db))).when(client).getAllTables(db);
    }
    for (HiveDatasetFinder.DbAndTable dbAndTable : dbAndTables) {
      Table table = new Table();
      table.setDbName(dbAndTable.getDb());
      table.setTableName(dbAndTable.getTable());
      Mockito.doReturn(table).when(client).getTable(dbAndTable.getDb(), dbAndTable.getTable());
    }

    AutoReturnableObject<IMetaStoreClient> aro = Mockito.mock(AutoReturnableObject.class);
    Mockito.when(aro.get()).thenReturn(client);

    Mockito.when(pool.getHiveRegProps()).thenReturn(null);
    Mockito.when(pool.getClient()).thenReturn(aro);
    return pool;
  }

  private class TestHiveDatasetFinder extends HiveDatasetFinder {

    public TestHiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool pool)
        throws IOException {
      super(fs, properties, pool);
    }

    @Override
    protected HiveDataset createHiveDataset(Table table)
        throws IOException {
      HiveDataset dataset = Mockito.mock(HiveDataset.class);
      Mockito.when(dataset.getTable()).thenReturn(new org.apache.hadoop.hive.ql.metadata.Table(table));
      return dataset;
    }
  }

}
