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

package org.apache.gobblin.data.management.conversion.hive.dataset;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.gobblin.data.management.conversion.hive.source.HiveAvroToOrcSource;
import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.copy.hive.HiveDatasetFinder;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.AutoReturnableObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class ConvertibleHiveDatasetFinderTest {
  private String avroConversionKeyPrefix = "hive.conversion.avro";
  private SetMultimap<String, String> entities = HashMultimap.create();
  private HiveMetastoreClientPool pool;

  @BeforeClass
  public void setUp() throws Exception {
    List<HiveDatasetFinder.DbAndTable> dbAndTables = Lists.newArrayList();
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table1"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table2"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1", "table3"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db2", "table1"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1_flattenedOrcDb", "table1_flattenedOrc_staging_xxxx"));
    dbAndTables.add(new HiveDatasetFinder.DbAndTable("db1_flattenedOrcDb", "table2_flattenedOrc_staging_yyyy"));

    for (HiveDatasetFinder.DbAndTable dbAndTable : dbAndTables) {
      entities.put(dbAndTable.getDb(), dbAndTable.getTable());
    }

    HiveMetastoreClientPool pool = Mockito.mock(HiveMetastoreClientPool.class);

    IMetaStoreClient client = Mockito.mock(IMetaStoreClient.class);
    Mockito.when(client.getAllDatabases()).thenReturn(Lists.newArrayList(entities.keySet()));
    for (String db : entities.keySet()) {
      Mockito.doReturn(Lists.newArrayList(entities.get(db))).when(client).getAllTables(db);
    }
    for (String db : entities.keySet()) {
      Set<String> tbs = entities.get(db);
      String targetDb = db + "_flattenedOrcDb";
      Set<String> candidates = entities.get(targetDb);
      for (String tb: tbs) {
        String stgTb = tb + "_flattenedOrc_staging";
        ArrayList<String> rst = new ArrayList<>();
        for (String c : candidates) {
          if (c.startsWith(stgTb)) {
            rst.add(c);
            Mockito.doAnswer(invocation -> {
                Object[] args = invocation.getArguments();
                String dropDB = (String)args[0];
                String dropTable = (String)args[1];
                entities.remove(dropDB, dropTable);
                return null;
            }).when(client).dropTable(targetDb, c, false, false);
          }
        }
        Mockito.doReturn(rst).when(client).getTables(targetDb, stgTb + "_*");
      }
    }

    for (HiveDatasetFinder.DbAndTable dbAndTable : dbAndTables) {
      Table table = new Table();
      table.setDbName(dbAndTable.getDb());
      table.setTableName(dbAndTable.getTable());
      StorageDescriptor sd = new StorageDescriptor();
      sd.setLocation("/tmp/test");
      table.setSd(sd);
      Mockito.doReturn(table).when(client).getTable(dbAndTable.getDb(), dbAndTable.getTable());
    }

    @SuppressWarnings("unchecked")
    AutoReturnableObject<IMetaStoreClient> aro = Mockito.mock(AutoReturnableObject.class);
    Mockito.when(aro.get()).thenReturn(client);

    Mockito.when(pool.getHiveRegProps()).thenReturn(null);
    Mockito.when(pool.getClient()).thenReturn(aro);
    this.pool = pool;
  }

  @Test
  public void testDatasetFinder() throws Exception {
      Assert.assertEquals(this.entities.size(), 6);
      Properties properties = new Properties();
      properties.put(HiveDatasetFinder.DB_KEY, "db1");
      properties.put(HiveDatasetFinder.TABLE_PATTERN_KEY, "table1|table2");
      properties.put(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, avroConversionKeyPrefix);
      properties.put(avroConversionKeyPrefix + "." + HiveAvroToOrcSource.CLEAN_STAGING_TABLES_IN_SEARCH,
              HiveAvroToOrcSource.DEFAULT_CLEAN_STAGING_TABLES_IN_SEARCH);
      HiveDatasetFinder finder = new TestConvertibleHiveDatasetFinder(FileSystem.getLocal(new Configuration()), properties, pool);
      List<HiveDataset> datasets = Lists.newArrayList(finder.getDatasetsIterator());
      Assert.assertEquals(datasets.size(), 2);
      Assert.assertEquals(this.entities.size(), 4);
  }

  class TestConvertibleHiveDatasetFinder extends ConvertibleHiveDatasetFinder {
    public TestConvertibleHiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool clientPool) throws IOException {
      super(fs, properties, clientPool);
    }

    @Override
    public Config getDatasetConfig(Table table) {
      String testConfFilePath = "convertibleHiveDatasetTest/flattenedOrc.conf";
      Config config = ConfigFactory.parseResources(testConfFilePath).getConfig(avroConversionKeyPrefix);
      return config;
    }
  }
}
