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

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.data.management.retention.version.HiveDatasetVersionCleaner;


public class HiveDatasetTest {

  private static String DUMMY_CONFIG_KEY_WITH_DB_TOKEN = "dummyConfig.withDB";
  private static String DUMMY_CONFIG_KEY_WITH_TABLE_TOKEN = "dummyConfig.withTable";
  private static Config config = ConfigFactory.parseMap(ImmutableMap.<String, String> of(
      HiveDatasetVersionCleaner.REPLACEMENT_HIVE_DB_NAME_KEY, "resPrefix_$LOGICAL_DB_resPostfix",
      HiveDatasetVersionCleaner.REPLACEMENT_HIVE_TABLE_NAME_KEY, "resPrefix_$LOGICAL_TABLE_resPostfix",
      DUMMY_CONFIG_KEY_WITH_DB_TOKEN, "resPrefix_$DB_resPostfix",
      DUMMY_CONFIG_KEY_WITH_TABLE_TOKEN, "resPrefix_$TABLE_resPostfix"));

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
        new HiveDatasetFinder.DbAndTable("myDB_dbPostfix", "myTable_tablePostfix"),
        HiveDataset.LOGICAL_DB_TOKEN, HiveDataset.LOGICAL_TABLE_TOKEN);
    Assert.assertEquals(logicalDbAndTable.getDb(), "myDB", "DB name not parsed correctly");
    Assert.assertEquals(logicalDbAndTable.getTable(), "myTable", "Table name not parsed correctly");

    // Happy Path - without postfix in DB and Table names
    datasetNamePattern = "dbPrefix_$LOGICAL_DB.tablePrefix_$LOGICAL_TABLE";
    logicalDbAndTable = HiveDataset.parseLogicalDbAndTable(datasetNamePattern,
        new HiveDatasetFinder.DbAndTable("dbPrefix_myDB", "tablePrefix_myTable"),
        HiveDataset.LOGICAL_DB_TOKEN, HiveDataset.LOGICAL_TABLE_TOKEN);
    Assert.assertEquals(logicalDbAndTable.getDb(), "myDB", "DB name not parsed correctly");
    Assert.assertEquals(logicalDbAndTable.getTable(), "myTable", "Table name not parsed correctly");

    // Happy Path - without any prefix and postfix in DB and Table names
    datasetNamePattern = "$LOGICAL_DB.$LOGICAL_TABLE";
    logicalDbAndTable = HiveDataset.parseLogicalDbAndTable(datasetNamePattern,
        new HiveDatasetFinder.DbAndTable("myDB", "myTable"),
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
    tokenValue = HiveDataset.extractTokenValueFromEntity("dbPrefix_myDB", "dbPrefix_$LOGICAL_DB",
        HiveDataset.LOGICAL_DB_TOKEN);
    Assert.assertEquals(tokenValue, "myDB", "DB name not extracted correctly");

    // Missing token in template
    try {
      tokenValue = HiveDataset.extractTokenValueFromEntity("dbPrefix_myDB_dbPostfix", "dbPrefix_$LOGICAL_TABLE_dbPostfix",
          HiveDataset.LOGICAL_DB_TOKEN);
      Assert.fail("Token is missing in template, code should have thrown exception");
    } catch (IllegalArgumentException e) {
      // Ignore exception, it was expected
    }

    // Missing source entity
    try {
      tokenValue = HiveDataset.extractTokenValueFromEntity("", "dbPrefix_$LOGICAL_DB_dbPostfix",
          HiveDataset.LOGICAL_DB_TOKEN);
      Assert.fail("Source entity is missing, code should have thrown exception");
    } catch (IllegalArgumentException e) {
      // Ignore exception, it was expected
    }

    // Missing template
    try {
      tokenValue = HiveDataset.extractTokenValueFromEntity("dbPrefix_myDB_dbPostfix", "",
          HiveDataset.LOGICAL_DB_TOKEN);
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

    Assert.assertEquals(resolvedConfig.getString(DUMMY_CONFIG_KEY_WITH_DB_TOKEN),
        "resPrefix_realDb_resPostfix", "Real DB not resolved correctly");
    Assert.assertEquals(resolvedConfig.getString(DUMMY_CONFIG_KEY_WITH_TABLE_TOKEN),
        "resPrefix_realTable_resPostfix", "Real Table not resolved correctly");

    Assert.assertEquals(resolvedConfig.getString(HiveDatasetVersionCleaner.REPLACEMENT_HIVE_DB_NAME_KEY),
        "resPrefix_logicalDb_resPostfix", "Logical DB not resolved correctly");
    Assert.assertEquals(resolvedConfig.getString(HiveDatasetVersionCleaner.REPLACEMENT_HIVE_TABLE_NAME_KEY),
        "resPrefix_logicalTable_resPostfix", "Logical Table not resolved correctly");
  }
}
