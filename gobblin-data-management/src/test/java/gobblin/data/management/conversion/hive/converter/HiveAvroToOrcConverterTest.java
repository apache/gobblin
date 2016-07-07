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

package gobblin.data.management.conversion.hive.converter;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.ConversionHiveTestUtils;
import gobblin.data.management.conversion.hive.LocalHiveMetastoreTestUtils;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDatasetTest;
import gobblin.data.management.conversion.hive.entities.QueryBasedHiveConversionEntity;
import gobblin.data.management.conversion.hive.entities.SchemaAwareHiveTable;


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

    Table table = this.hiveMetastoreTestUtils.createTestTable(dbName, tableName, tableSdLoc, Optional.<String> absent());
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir, "recordWithinRecordWithinRecord_nested.json");
    WorkUnitState wus = ConversionHiveTestUtils.createWus(dbName, tableName, 0);

    try (HiveAvroToFlattenedOrcConverter converter = new HiveAvroToFlattenedOrcConverter();) {

      Config config = ConfigFactory.parseMap(ImmutableMap.<String, String> builder()
          .put("destinationFormats", "flattenedOrc")
          .put("flattenedOrc.destination.dbName",dbName)
          .put("flattenedOrc.destination.tableName", tableName + "_orc")
          .put("flattenedOrc.destination.dataPath","file:" + tableSdLoc + "_orc")
          .build());

      ConvertibleHiveDataset cd = ConvertibleHiveDatasetTest.createTestConvertibleDataset(config);

      List<QueryBasedHiveConversionEntity> conversionEntities =
          Lists.newArrayList(converter.convertRecord(converter.convertSchema(schema, wus), new QueryBasedHiveConversionEntity(cd, new SchemaAwareHiveTable(table, schema)), wus));

      Assert.assertEquals(conversionEntities.size(), 1, "Only one query entity should be returned");

      QueryBasedHiveConversionEntity queryBasedHiveConversionEntity = conversionEntities.get(0);
      List<String> queries = queryBasedHiveConversionEntity.getQueries();

      Assert.assertEquals(queries.size(), 2, "One DDL and one DML query should be returned");

      Assert.assertEquals(queries.get(0).trim().replaceAll(" ", ""),
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_flattened.ddl").replaceAll(" ", ""));
      Assert.assertEquals(queries.get(1).trim().replaceAll(" ", ""),
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_flattened.dml").replaceAll(" ", ""));
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

    Table table = this.hiveMetastoreTestUtils.createTestTable(dbName, tableName, tableSdLoc, Optional.<String> absent());
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

      Assert.assertEquals(queries.size(), 2, "One DDL and one DML query should be returned");

      Assert.assertEquals(queries.get(0).trim().replaceAll(" ", ""),
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_nested.ddl").replaceAll(" ", ""));
      Assert.assertEquals(queries.get(1).trim().replaceAll(" ", ""),
          ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_nested.dml").replaceAll(" ", ""));
    }
  }

}
