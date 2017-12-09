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

package org.apache.gobblin.data.management.conversion.hive.converter;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.data.management.ConversionHiveTestUtils;
import org.apache.gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import org.apache.gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDatasetTest;
import org.apache.gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import org.apache.gobblin.data.management.conversion.hive.entities.SchemaAwareHivePartition;
import org.apache.gobblin.data.management.conversion.hive.entities.SchemaAwareHiveTable;
import org.apache.gobblin.data.management.copy.hive.WhitelistBlacklist;


@Test(groups = { "gobblin.data.management.conversion" })
public class HiveAvroToOrcConverterTest {

  private static String resourceDir = "hiveConverterTest";
  private LocalHiveMetastoreTestUtils hiveMetastoreTestUtils;

  public HiveAvroToOrcConverterTest() {
    this.hiveMetastoreTestUtils = LocalHiveMetastoreTestUtils.getInstance();
  }

  /***
   * Test flattened DDL and DML generation
   * @throws IOException
   */
  @Test
  public void testFlattenSchemaDDLandDML() throws Exception {
    String dbName = "testdb";
    String tableName = "testtable";
    String tableSdLoc = "/tmp/testtable";

    this.hiveMetastoreTestUtils.getLocalMetastoreClient().dropDatabase(dbName, false, true, true);

    Table table = this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName, tableSdLoc, Optional.<String> absent());
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir, "recordWithinRecordWithinRecord_nested.json");
    WorkUnitState wus = ConversionHiveTestUtils.createWus(dbName, tableName, 0);

    try (HiveAvroToFlattenedOrcConverter converter = new HiveAvroToFlattenedOrcConverter();) {

      Config config = ConfigFactory.parseMap(
          ImmutableMap.<String, String>builder().put("destinationFormats", "flattenedOrc")
              .put("flattenedOrc.destination.dbName", dbName)
              .put("flattenedOrc.destination.tableName", tableName + "_orc")
              .put("flattenedOrc.destination.dataPath", "file:" + tableSdLoc + "_orc").build());

      ConvertibleHiveDataset cd = ConvertibleHiveDatasetTest.createTestConvertibleDataset(config);

      List<QueryBasedHiveConversionEntity> conversionEntities =
          Lists.newArrayList(converter.convertRecord(converter.convertSchema(schema, wus),
              new QueryBasedHiveConversionEntity(cd, new SchemaAwareHiveTable(table, schema)), wus));

      Assert.assertEquals(conversionEntities.size(), 1, "Only one query entity should be returned");

      QueryBasedHiveConversionEntity queryBasedHiveConversionEntity = conversionEntities.get(0);
      List<String> queries = queryBasedHiveConversionEntity.getQueries();

      Assert.assertEquals(queries.size(), 4, "4 DDL and one DML query should be returned");

      // Ignoring part before first bracket in DDL and 'select' clause in DML because staging table has
      // .. a random name component
      String actualDDLQuery = StringUtils.substringAfter("(", queries.get(0).trim());
      String actualDMLQuery = StringUtils.substringAfter("SELECT", queries.get(0).trim());
      String expectedDDLQuery = StringUtils.substringAfter("(",
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_flattened.ddl"));
      String expectedDMLQuery = StringUtils.substringAfter("SELECT",
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_flattened.dml"));

      Assert.assertEquals(actualDDLQuery, expectedDDLQuery);
      Assert.assertEquals(actualDMLQuery, expectedDMLQuery);
    }

  }

  /***
   * Test nested DDL and DML generation
   * @throws IOException
   */
  @Test
  public void testNestedSchemaDDLandDML() throws Exception {
    String dbName = "testdb";
    String tableName = "testtable";
    String tableSdLoc = "/tmp/testtable";

    this.hiveMetastoreTestUtils.getLocalMetastoreClient().dropDatabase(dbName, false, true, true);

    Table table = this.hiveMetastoreTestUtils.createTestAvroTable(dbName, tableName, tableSdLoc, Optional.<String> absent());
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir, "recordWithinRecordWithinRecord_nested.json");
    WorkUnitState wus = ConversionHiveTestUtils.createWus(dbName, tableName, 0);
    wus.getJobState().setProp("orc.table.flatten.schema", "false");

    try (HiveAvroToNestedOrcConverter converter = new HiveAvroToNestedOrcConverter();) {

      Config config = ConfigFactory.parseMap(ImmutableMap.<String, String> builder()
          .put("destinationFormats", "nestedOrc")
          .put("nestedOrc.destination.tableName","testtable_orc_nested")
          .put("nestedOrc.destination.dbName",dbName)
          .put("nestedOrc.destination.dataPath","file:/tmp/testtable_orc_nested")
          .build());

      ConvertibleHiveDataset cd = ConvertibleHiveDatasetTest.createTestConvertibleDataset(config);

      List<QueryBasedHiveConversionEntity> conversionEntities =
          Lists.newArrayList(converter.convertRecord(converter.convertSchema(schema, wus), new QueryBasedHiveConversionEntity(cd , new SchemaAwareHiveTable(table, schema)), wus));

      Assert.assertEquals(conversionEntities.size(), 1, "Only one query entity should be returned");

      QueryBasedHiveConversionEntity queryBasedHiveConversionEntity = conversionEntities.get(0);
      List<String> queries = queryBasedHiveConversionEntity.getQueries();

      Assert.assertEquals(queries.size(), 4, "4 DDL and one DML query should be returned");

      // Ignoring part before first bracket in DDL and 'select' clause in DML because staging table has
      // .. a random name component
      String actualDDLQuery = StringUtils.substringAfter("(", queries.get(0).trim());
      String actualDMLQuery = StringUtils.substringAfter("SELECT", queries.get(0).trim());
      String expectedDDLQuery = StringUtils.substringAfter("(",
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_nested.ddl"));
      String expectedDMLQuery = StringUtils.substringAfter("SELECT",
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_nested.dml"));

      Assert.assertEquals(actualDDLQuery, expectedDDLQuery);
      Assert.assertEquals(actualDMLQuery, expectedDMLQuery);
    }
  }

  @Test
  public void dropReplacedPartitionsTest() throws Exception {

    Table table = ConvertibleHiveDatasetTest.getTestTable("dbName", "tableName");
    table.setTableType("VIRTUAL_VIEW");
    table.setPartitionKeys(ImmutableList.of(new FieldSchema("year", "string", ""), new FieldSchema("month", "string", "")));

    Partition part = new Partition();
    part.setParameters(ImmutableMap.of("gobblin.replaced.partitions", "2015,12|2016,01"));

    SchemaAwareHiveTable hiveTable = new SchemaAwareHiveTable(table, null);
    SchemaAwareHivePartition partition = new SchemaAwareHivePartition(table, part, null);

    QueryBasedHiveConversionEntity conversionEntity = new QueryBasedHiveConversionEntity(null, hiveTable, Optional.of(partition));
    List<ImmutableMap<String, String>> expected =
        ImmutableList.of(ImmutableMap.of("year", "2015", "month", "12"), ImmutableMap.of("year", "2016", "month", "01"));
    Assert.assertEquals(AbstractAvroToOrcConverter.getDropPartitionsDDLInfo(conversionEntity), expected);

    // Make sure that a partition itself is not dropped
    Partition replacedSelf = new Partition();
    replacedSelf.setParameters(ImmutableMap.of("gobblin.replaced.partitions", "2015,12|2016,01|2016,02"));
    replacedSelf.setValues(ImmutableList.of("2016", "02"));

    conversionEntity = new QueryBasedHiveConversionEntity(null, hiveTable, Optional.of(new SchemaAwareHivePartition(table, replacedSelf, null)));
    Assert.assertEquals(AbstractAvroToOrcConverter.getDropPartitionsDDLInfo(conversionEntity), expected);
  }

  @Test
  /***
   * More comprehensive tests for WhiteBlackList are in: {@link org.apache.gobblin.data.management.copy.hive.WhitelistBlacklistTest}
   */
  public void hiveViewRegistrationWhiteBlackListTest() throws Exception {
    WorkUnitState wus = ConversionHiveTestUtils.createWus("dbName", "tableName", 0);

    Optional<WhitelistBlacklist> optionalWhitelistBlacklist = AbstractAvroToOrcConverter.getViewWhiteBackListFromWorkUnit(wus);
    Assert.assertTrue(!optionalWhitelistBlacklist.isPresent(),
        "No whitelist blacklist specified in WUS, WhiteListBlackList object should be absent");

    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_WHITELIST, "");
    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_BLACKLIST, "");
    optionalWhitelistBlacklist = AbstractAvroToOrcConverter.getViewWhiteBackListFromWorkUnit(wus);
    Assert.assertTrue(optionalWhitelistBlacklist.isPresent(),
        "Whitelist blacklist specified in WUS, WhiteListBlackList object should be present");
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptDb("mydb"));
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptTable("mydb", "mytable"));

    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_WHITELIST, "yourdb");
    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_BLACKLIST, "");
    optionalWhitelistBlacklist = AbstractAvroToOrcConverter.getViewWhiteBackListFromWorkUnit(wus);
    Assert.assertTrue(optionalWhitelistBlacklist.isPresent(),
        "Whitelist blacklist specified in WUS, WhiteListBlackList object should be present");
    Assert.assertTrue(!optionalWhitelistBlacklist.get().acceptDb("mydb"));
    Assert.assertTrue(!optionalWhitelistBlacklist.get().acceptTable("mydb", "mytable"));
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptDb("yourdb"));
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptTable("yourdb", "mytable"));

    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_WHITELIST, "yourdb.yourtable");
    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_BLACKLIST, "");
    optionalWhitelistBlacklist = AbstractAvroToOrcConverter.getViewWhiteBackListFromWorkUnit(wus);
    Assert.assertTrue(optionalWhitelistBlacklist.isPresent(),
        "Whitelist blacklist specified in WUS, WhiteListBlackList object should be present");
    Assert.assertTrue(!optionalWhitelistBlacklist.get().acceptDb("mydb"));
    Assert.assertTrue(!optionalWhitelistBlacklist.get().acceptTable("yourdb", "mytable"));
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptDb("yourdb"));
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptTable("yourdb", "yourtable"));

    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_WHITELIST, "");
    wus.setProp(AbstractAvroToOrcConverter.HIVE_CONVERSION_VIEW_REGISTRATION_BLACKLIST, "yourdb.yourtable");
    optionalWhitelistBlacklist = AbstractAvroToOrcConverter.getViewWhiteBackListFromWorkUnit(wus);
    Assert.assertTrue(optionalWhitelistBlacklist.isPresent(),
        "Whitelist blacklist specified in WUS, WhiteListBlackList object should be present");
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptDb("mydb"));
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptTable("yourdb", "mytable"));
    Assert.assertTrue(optionalWhitelistBlacklist.get().acceptDb("yourdb"));
    Assert.assertTrue(!optionalWhitelistBlacklist.get().acceptTable("yourdb", "yourtable"));
  }
}
