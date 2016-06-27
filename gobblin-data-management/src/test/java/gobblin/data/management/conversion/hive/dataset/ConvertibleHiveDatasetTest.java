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
package gobblin.data.management.conversion.hive.dataset;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.hive.HiveMetastoreClientPool;


public class ConvertibleHiveDatasetTest {

  @Test
  public void testConstructor() throws Exception {
    Config config =
        ConfigFactory.parseMap(ImmutableMap.of(ConvertibleHiveDataset.DESTINATION_DB_KEY, "dest_db",
            ConvertibleHiveDataset.DESTINATION_TABLE_KEY, "dest_tb",
            ConvertibleHiveDataset.DESTINATION_DATA_PATH_KEY, "dest_data",
            ConvertibleHiveDataset.DESTINATION_CONVERSION_FORMATS_KEY, "a,b"));

    Table table = getTestTable("db1", "tb1");
    ConvertibleHiveDataset cd =
        new ConvertibleHiveDataset(Mockito.mock(FileSystem.class), Mockito.mock(HiveMetastoreClientPool.class), new org.apache.hadoop.hive.ql.metadata.Table(
            table), config);
    Assert.assertEquals(cd.getDestinationDbName().get(), "dest_db");
    Assert.assertEquals(cd.getDestinationTableName().get(), "dest_tb");
    Assert.assertEquals(cd.getDestinationDataPath().get(), "dest_data");
    Assert.assertEquals(cd.getFormats().get(), ImmutableSet.of("a", "b"));
  }

  /**
   * Test to make sure {dbName} and {tableName} are resolved
   * @throws Exception
   */
  @Test
  public void testSubstitution() throws Exception {
    Config config =
        ConfigFactory.parseMap(ImmutableMap.of(ConvertibleHiveDataset.DESTINATION_DB_KEY, "dest_{dbName}",
            ConvertibleHiveDataset.DESTINATION_TABLE_KEY, "dest_{tableName}",
            ConvertibleHiveDataset.DESTINATION_DATA_PATH_KEY,"dest_data/{dbName}/{tableName}"));

    Table table = getTestTable("db1", "tb1");
    ConvertibleHiveDataset cd =
        new ConvertibleHiveDataset(Mockito.mock(FileSystem.class), Mockito.mock(HiveMetastoreClientPool.class), new org.apache.hadoop.hive.ql.metadata.Table(
            table), config);
    Assert.assertEquals(cd.getDestinationDbName().get(), "dest_db1");
    Assert.assertEquals(cd.getDestinationTableName().get(), "dest_tb1");
    Assert.assertEquals(cd.getDestinationDataPath().get(), "dest_data/db1/tb1");
  }

  private Table getTestTable(String dbName, String tableName) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation("/tmp/test");
    table.setSd(sd);
    return table;
  }
}
