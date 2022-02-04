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
package org.apache.gobblin.data.management.copy.hive;

import com.google.common.base.Optional;
import java.io.IOException;

import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.data.management.copy.CopyConfiguration;
import org.apache.gobblin.hive.HiveMetastoreClientPool;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.retention.version.HiveDatasetVersionCleaner;


public class HiveDatasetTest {

  private static String DUMMY_CONFIG_KEY_WITH_DB_TOKEN = "dummyConfig.withDB";
  private static String DUMMY_CONFIG_KEY_WITH_STRIP_SUFFIX = "dummyConfig" + ConfigUtils.STRIP_SUFFIX;
  private static String DUMMY_CONFIG_KEY_WITH_TABLE_TOKEN = "dummyConfig.withTable";
  private static Config config = ConfigFactory.parseMap(
      ImmutableMap.<String, String>builder().put(DUMMY_CONFIG_KEY_WITH_STRIP_SUFFIX, "testRoot")
          .put(HiveDatasetVersionCleaner.REPLACEMENT_HIVE_DB_NAME_KEY, "resPrefix_$LOGICAL_DB_resPostfix")
          .put(HiveDatasetVersionCleaner.REPLACEMENT_HIVE_TABLE_NAME_KEY, "resPrefix_$LOGICAL_TABLE_resPostfix")
          .put(DUMMY_CONFIG_KEY_WITH_DB_TOKEN, "resPrefix_$DB_resPostfix")
          .put(DUMMY_CONFIG_KEY_WITH_TABLE_TOKEN, "resPrefix_$TABLE_resPostfix")
          .build());

  @Test
  public void testParseLogicalDbAndTable() throws IOException {

    String datasetNamePattern;
    HiveDatasetFinder.DbAndTable logicalDbAndTable;

    // Happy Path
    datasetNamePattern = "dbPrefix_$LOGICAL_DB_dbPostfix.tablePrefix_$LOGICAL_TABLE_tablePostfix";
    logicalDbAndTable = HiveDataset.parseLogicalDbAndTable(datasetNamePattern,
        new HiveDatasetFinder.DbAndTable("dbPrefix_myDB_dbPostfix", "tablePrefix_myTable_tablePostfix"),
        HiveDataset.LOGICAL_DB_TOKEN, HiveDataset.LOGICAL_TABLE_TOKEN);
    Assert.assertEquals(logicalDbAndTable.getDb(), "myDB", "DB name not parsed correctly");
    Assert.assertEquals(logicalDbAndTable.getTable(), "myTable", "Table name not parsed correctly");

    // Happy Path - without prefix in DB and Table names
    datasetNamePattern = "$LOGICAL_DB_dbPostfix.$LOGICAL_TABLE_tablePostfix";
    logicalDbAndTable = HiveDataset.parseLogicalDbAndTable(datasetNamePattern,
        new HiveDatasetFinder.DbAndTable("myDB_dbPostfix", "myTable_tablePostfix"), HiveDataset.LOGICAL_DB_TOKEN,
        HiveDataset.LOGICAL_TABLE_TOKEN);
    Assert.assertEquals(logicalDbAndTable.getDb(), "myDB", "DB name not parsed correctly");
    Assert.assertEquals(logicalDbAndTable.getTable(), "myTable", "Table name not parsed correctly");

    // Happy Path - without postfix in DB and Table names
    datasetNamePattern = "dbPrefix_$LOGICAL_DB.tablePrefix_$LOGICAL_TABLE";
    logicalDbAndTable = HiveDataset.parseLogicalDbAndTable(datasetNamePattern,
        new HiveDatasetFinder.DbAndTable("dbPrefix_myDB", "tablePrefix_myTable"), HiveDataset.LOGICAL_DB_TOKEN,
        HiveDataset.LOGICAL_TABLE_TOKEN);
    Assert.assertEquals(logicalDbAndTable.getDb(), "myDB", "DB name not parsed correctly");
    Assert.assertEquals(logicalDbAndTable.getTable(), "myTable", "Table name not parsed correctly");

    // Happy Path - without any prefix and postfix in DB and Table names
    datasetNamePattern = "$LOGICAL_DB.$LOGICAL_TABLE";
    logicalDbAndTable =
        HiveDataset.parseLogicalDbAndTable(datasetNamePattern, new HiveDatasetFinder.DbAndTable("myDB", "myTable"),
            HiveDataset.LOGICAL_DB_TOKEN, HiveDataset.LOGICAL_TABLE_TOKEN);
    Assert.assertEquals(logicalDbAndTable.getDb(), "myDB", "DB name not parsed correctly");
    Assert.assertEquals(logicalDbAndTable.getTable(), "myTable", "Table name not parsed correctly");

    // Dataset name pattern missing
    datasetNamePattern = "";
    try {
      logicalDbAndTable = HiveDataset.parseLogicalDbAndTable(datasetNamePattern,
          new HiveDatasetFinder.DbAndTable("dbPrefix_myDB_dbPostfix", "tablePrefix_myTable_tablePostfix"),
          HiveDataset.LOGICAL_DB_TOKEN, HiveDataset.LOGICAL_TABLE_TOKEN);
      Assert.fail("Dataset name pattern is missing, code should have thrown exception");
    } catch (IllegalArgumentException e) {
      // Ignore exception, it was expected
    }

    // Malformed Dataset name pattern
    datasetNamePattern = "dbPrefix_$LOGICAL_DB_dbPostfixtablePrefix_$LOGICAL_TABLE_tablePostfix";
    try {
      logicalDbAndTable = HiveDataset.parseLogicalDbAndTable(datasetNamePattern,
          new HiveDatasetFinder.DbAndTable("dbPrefix_myDB_dbPostfix", "tablePrefix_myTable_tablePostfix"),
          HiveDataset.LOGICAL_DB_TOKEN, HiveDataset.LOGICAL_TABLE_TOKEN);
      Assert.fail("Dataset name pattern is missing, code should have thrown exception");
    } catch (IllegalArgumentException e) {
      // Ignore exception, it was expected
    }
  }

  @Test
  public void testExtractTokenValueFromEntity() throws IOException {

    String tokenValue;

    // Happy Path
    tokenValue = HiveDataset.extractTokenValueFromEntity("dbPrefix_myDB_dbPostfix", "dbPrefix_$LOGICAL_DB_dbPostfix",
        HiveDataset.LOGICAL_DB_TOKEN);
    Assert.assertEquals(tokenValue, "myDB", "DB name not extracted correctly");

    // Happy Path - without prefix
    tokenValue = HiveDataset.extractTokenValueFromEntity("myDB_dbPostfix", "$LOGICAL_DB_dbPostfix",
        HiveDataset.LOGICAL_DB_TOKEN);
    Assert.assertEquals(tokenValue, "myDB", "DB name not extracted correctly");

    // Happy Path - without postfix
    tokenValue = HiveDataset.extractTokenValueFromEntity("dbPrefix_myDB", "dbPrefix_$LOGICAL_DB", HiveDataset.LOGICAL_DB_TOKEN);
    Assert.assertEquals(tokenValue, "myDB", "DB name not extracted correctly");

    // Missing token in template
    try {
      tokenValue =
          HiveDataset.extractTokenValueFromEntity("dbPrefix_myDB_dbPostfix", "dbPrefix_$LOGICAL_TABLE_dbPostfix",
              HiveDataset.LOGICAL_DB_TOKEN);
      Assert.fail("Token is missing in template, code should have thrown exception");
    } catch (IllegalArgumentException e) {
      // Ignore exception, it was expected
    }

    // Missing source entity
    try {
      tokenValue = HiveDataset.extractTokenValueFromEntity("", "dbPrefix_$LOGICAL_DB_dbPostfix", HiveDataset.LOGICAL_DB_TOKEN);
      Assert.fail("Source entity is missing, code should have thrown exception");
    } catch (IllegalArgumentException e) {
      // Ignore exception, it was expected
    }

    // Missing template
    try {
      tokenValue = HiveDataset.extractTokenValueFromEntity("dbPrefix_myDB_dbPostfix", "", HiveDataset.LOGICAL_DB_TOKEN);
      Assert.fail("Template is missing, code should have thrown exception");
    } catch (IllegalArgumentException e) {
      // Ignore exception, it was expected
    }
  }

  @Test
  public void testResolveConfig() throws IOException {
    HiveDatasetFinder.DbAndTable realDbAndTable = new HiveDatasetFinder.DbAndTable("realDb", "realTable");
    HiveDatasetFinder.DbAndTable logicalDbAndTable = new HiveDatasetFinder.DbAndTable("logicalDb", "logicalTable");
    Config resolvedConfig = HiveDataset.resolveConfig(config, realDbAndTable, logicalDbAndTable);

    Assert.assertEquals(resolvedConfig.getString(DUMMY_CONFIG_KEY_WITH_DB_TOKEN), "resPrefix_realDb_resPostfix",
        "Real DB not resolved correctly");
    Assert.assertEquals(resolvedConfig.getString(DUMMY_CONFIG_KEY_WITH_TABLE_TOKEN), "resPrefix_realTable_resPostfix",
        "Real Table not resolved correctly");

    Assert.assertEquals(resolvedConfig.getString(HiveDatasetVersionCleaner.REPLACEMENT_HIVE_DB_NAME_KEY),
        "resPrefix_logicalDb_resPostfix", "Logical DB not resolved correctly");
    Assert.assertEquals(resolvedConfig.getString(HiveDatasetVersionCleaner.REPLACEMENT_HIVE_TABLE_NAME_KEY),
        "resPrefix_logicalTable_resPostfix", "Logical Table not resolved correctly");
    Assert.assertEquals(resolvedConfig.getString(DUMMY_CONFIG_KEY_WITH_STRIP_SUFFIX), "testRoot");
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testThrowsErrorIfCopyEntityHelperFails() throws Exception {
    Properties copyProperties = new Properties();
    Properties hiveProperties = new Properties();
    copyProperties.put(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target");
    // Invoke an IOException by passing in a class that does not exist to HiveCopyEntityHelper constructor
    hiveProperties.put(HiveCopyEntityHelper.COPY_PARTITION_FILTER_GENERATOR, "missingClass");
    Table table = new Table(Table.getEmptyTable("testDB", "testTable"));
    HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(new Properties(), Optional.absent());

    HiveDataset passingDataset = new HiveDataset(new LocalFileSystem(), pool, table, hiveProperties);
    // Even though IOException is thrown HiveDataset should silence it due to not having the configuration flag
    try {
      CopyConfiguration copyConfigWithoutAbortKey = CopyConfiguration.builder(new LocalFileSystem(), copyProperties).build();
      passingDataset.getFileSetIterator(FileSystem.getLocal(new Configuration()), copyConfigWithoutAbortKey);
    } catch (Exception e) {
      Assert.fail("IOException should log and fail silently since it is not configured");
    }

    // Exception should propagate to a RuntimeException since flag is enabled
    copyProperties.put(CopyConfiguration.ABORT_ON_SINGLE_DATASET_FAILURE, "true");
    HiveDataset failingDataset = new HiveDataset(new LocalFileSystem(), pool, table, hiveProperties);
    CopyConfiguration copyConfiguration = CopyConfiguration.builder(new LocalFileSystem(), copyProperties).build();
    failingDataset.getFileSetIterator(FileSystem.getLocal(new Configuration()), copyConfiguration);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testThrowsErrorIfTableNotCopyable() throws Exception {
    Properties copyProperties = new Properties();
    Properties hiveProperties = new Properties();
    copyProperties.put(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/target");
    Table table = new Table(Table.getEmptyTable("testDB", "testTable"));
    // Virtual view tables are not copyable
    table.setTableType(TableType.VIRTUAL_VIEW);
    HiveMetastoreClientPool pool = HiveMetastoreClientPool.get(new Properties(), Optional.absent());

    HiveDataset passingDataset = new HiveDataset(new LocalFileSystem(), pool, table, hiveProperties);
    // Since flag is not enabled the dataset should log an error and continue
    try {
      CopyConfiguration copyConfigWithoutAbortKey = CopyConfiguration.builder(new LocalFileSystem(), copyProperties).build();
      passingDataset.getFileSetIterator(FileSystem.getLocal(new Configuration()), copyConfigWithoutAbortKey);
    } catch (Exception e) {
      Assert.fail("IOException should log and fail silently since it is not configured");
    }

    // Exception should propagate to a RuntimeException since flag is enabled
    copyProperties.put(CopyConfiguration.ABORT_ON_SINGLE_DATASET_FAILURE, "true");
    HiveDataset failingDataset = new HiveDataset(new LocalFileSystem(), pool, table, hiveProperties);
    CopyConfiguration copyConfiguration = CopyConfiguration.builder(new LocalFileSystem(), copyProperties).build();
    failingDataset.getFileSetIterator(FileSystem.getLocal(new Configuration()), copyConfiguration);
  }
}