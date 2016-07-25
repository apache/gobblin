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

package gobblin.data.management.conversion.hive.util;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import gobblin.data.management.ConversionHiveTestUtils;
import gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import gobblin.util.AvroFlattener;


@Test(groups = { "gobblin.data.management.conversion" })
public class HiveAvroORCQueryGeneratorTest {

  private static String resourceDir = "avroToOrcQueryUtilsTest";
  private static Optional<Table> destinationTableMeta = Optional.absent();
  private static boolean isEvolutionEnabled = true;
  private static Optional<Integer> rowLimit = Optional.absent();

  /***
   * Test DDL generation for schema structured as: Array within record within array within record
   * @throws IOException
   */
  @Test
  public void testArrayWithinRecordWithinArrayWithinRecordDDL() throws IOException {
    String schemaName = "testArrayWithinRecordWithinArrayWithinRecordDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir,
        "arrayWithinRecordWithinArrayWithinRecord_nested.json");

    String q = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(schema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            Optional.<Map<String, String>>absent(), isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(q,
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "arrayWithinRecordWithinArrayWithinRecord_nested.ddl"));
  }

  /***
   * Test DDL generation for schema structured as: option within option within record
   * @throws IOException
   */
  @Test
  public void testOptionWithinOptionWithinRecordDDL() throws IOException {
    String schemaName = "testOptionWithinOptionWithinRecordDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir,
        "optionWithinOptionWithinRecord_nested.json");

    String q = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(schema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            Optional.<Map<String, String>>absent(), isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(q,
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "optionWithinOptionWithinRecord_nested.ddl"));
  }

  /***
   * Test DDL generation for schema structured as: record within option within record
   * @throws IOException
   */
  @Test
  public void testRecordWithinOptionWithinRecordDDL() throws IOException {
    String schemaName = "testRecordWithinOptionWithinRecordDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir,
        "recordWithinOptionWithinRecord_nested.json");

    String q = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(schema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            Optional.<Map<String, String>>absent(), isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(q.trim(),
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinOptionWithinRecord_nested.ddl"));
  }

  /***
   * Test DDL generation for schema structured as: record within record within record
   * @throws IOException
   */
  @Test
  public void testRecordWithinRecordWithinRecordDDL() throws IOException {
    String schemaName = "testRecordWithinRecordWithinRecordDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir,
        "recordWithinRecordWithinRecord_nested.json");

    String q = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(schema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            Optional.<Map<String, String>>absent(), isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(q.trim(),
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_nested.ddl"));
  }

  /***
   * Test DDL generation for schema structured as: record within record within record after flattening
   * @throws IOException
   */
  @Test
  public void testRecordWithinRecordWithinRecordFlattenedDDL() throws IOException {
    String schemaName = "testRecordWithinRecordWithinRecordDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir, "recordWithinRecordWithinRecord_nested.json");

    AvroFlattener avroFlattener = new AvroFlattener();
    Schema flattenedSchema = avroFlattener.flatten(schema, true);

    String q = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(flattenedSchema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            Optional.<Map<String, String>>absent(), isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(q,
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord_flattened.ddl"));
  }

  /***
   * Test DML generation
   * @throws IOException
   */
  @Test
  public void testRecordWithinRecordWithinRecordFlattenedDML() throws IOException {
    String schemaName = "testRecordWithinRecordWithinRecordDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir,
        "recordWithinRecordWithinRecord_nested.json");

    AvroFlattener avroFlattener = new AvroFlattener();
    Schema flattenedSchema = avroFlattener.flatten(schema, true);

    String q = HiveAvroORCQueryGenerator
        .generateTableMappingDML(schema, flattenedSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(q.trim(),
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "recordWithinRecordWithinRecord.dml"));
  }

  /***
   * Test bad schema
   * @throws IOException
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonRecordRootSchemaDDL() throws Exception {
    String schemaName = "nonRecordRootSchema";
    Schema schema = Schema.create(Schema.Type.STRING);

    HiveAvroORCQueryGenerator
        .generateCreateTableDDL(schema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            Optional.<Map<String, String>>absent(), isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());
  }

  /***
   * Test DML generation with row limit
   * @throws IOException
   */
  @Test
  public void testFlattenedDMLWithRowLimit() throws IOException {
    String schemaName = "testRecordWithinRecordWithinRecordDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir,
        "recordWithinRecordWithinRecord_nested.json");
    Optional<Integer> rowLimit = Optional.of(1);

    AvroFlattener avroFlattener = new AvroFlattener();
    Schema flattenedSchema = avroFlattener.flatten(schema, true);

    String q = HiveAvroORCQueryGenerator
        .generateTableMappingDML(schema, flattenedSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(q.trim(),
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "flattenedWithRowLimit.dml"));
  }

  @Test
  public void testDropPartitions() throws Exception {

    List<Map<String, String>> partitionDMLInfos = Lists.newArrayList();
    partitionDMLInfos.add(ImmutableMap.of("datepartition", "2016-01-01", "sizepartition", "10"));
    partitionDMLInfos.add(ImmutableMap.of("datepartition", "2016-01-02", "sizepartition", "20"));
    partitionDMLInfos.add(ImmutableMap.of("datepartition", "2016-01-03", "sizepartition", "30"));

    List<String> ddl = HiveAvroORCQueryGenerator.generateDropPartitionsDDL("db1", "table1", partitionDMLInfos);

    Assert.assertEquals(ddl.size(), 2);
    Assert.assertEquals(ddl.get(0), "USE db1 \n");
    Assert.assertEquals(ddl.get(1),
          "ALTER TABLE table1 DROP IF EXISTS  PARTITION (datepartition='2016-01-01',sizepartition='10'), "
        + "PARTITION (datepartition='2016-01-02',sizepartition='20'), "
        + "PARTITION (datepartition='2016-01-03',sizepartition='30')");

    // Check empty partitions
    Assert.assertEquals(HiveAvroORCQueryGenerator.generateDropPartitionsDDL("db1", "table1",
        Collections.<Map<String, String>>emptyList()), Collections.emptyList());
  }

  @Test
  public void testCreatePartitionDDL() throws Exception {
    List<String> ddl = HiveAvroORCQueryGenerator.generateCreatePartitionDDL("db1", "table1", "/tmp",
        ImmutableMap.of("datepartition", "2016-01-01", "sizepartition", "10"));

    Assert.assertEquals(ddl.size(), 2);
    Assert.assertEquals(ddl.get(0), "USE db1\n");
    Assert.assertEquals(ddl.get(1),
        "ALTER TABLE `table1` ADD IF NOT EXISTS PARTITION (`datepartition`='2016-01-01', `sizepartition`='10') \n"
            + " LOCATION '/tmp' ");
  }

  @Test
  public void testDropTableDDL() throws Exception {
    String ddl = HiveAvroORCQueryGenerator.generateDropTableDDL("db1", "table1");

    Assert.assertEquals(ddl, "DROP TABLE IF EXISTS `db1`.`table1`");
  }
}
