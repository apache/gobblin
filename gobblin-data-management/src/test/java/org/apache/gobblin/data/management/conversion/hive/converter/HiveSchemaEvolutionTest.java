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
package gobblin.data.management.conversion.hive.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import gobblin.data.management.ConversionHiveTestUtils;
import gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import gobblin.util.AvroFlattener;


/**
 * Test for schema evolution enabled or disabled
 */
@Slf4j
@Test(groups = { "gobblin.data.management.conversion" })
public class HiveSchemaEvolutionTest {
  private static String resourceDir = "avroToOrcSchemaEvolutionTest";
  private static String schemaName = "sourceSchema";
  private static String hiveDbName = "hiveDb";
  private static AvroFlattener avroFlattener = new AvroFlattener();
  private static Schema inputSchema;
  private static Schema outputSchema;
  private static Optional<Integer> rowLimit = Optional.absent();

  static {
    try {
      inputSchema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir, "source_schema.json");
      outputSchema = avroFlattener.flatten(inputSchema, true);
    } catch (IOException e) {
      throw new RuntimeException("Could not initialized tests", e);
    }
  }

  @Test
  public void testEvolutionEnabledForExistingTable() throws IOException {
    boolean isEvolutionEnabled = true;
    Optional<Table> destinationTableMeta = createEvolvedDestinationTable(schemaName, "default", "", true);

    String ddl = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(outputSchema,
            schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            null, isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.ddl"), "Generated DDL did not match expected for evolution enabled");

    String dml = HiveAvroORCQueryGenerator
        .generateTableMappingDML(inputSchema, outputSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.dml"), "Generated DML did not match expected for evolution enabled");
  }

  @Test
  public void testEvolutionEnabledForNewTable() throws IOException {
    boolean isEvolutionEnabled = true;
    Optional<Table> destinationTableMeta = Optional.absent();

    String ddl = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(outputSchema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            null, isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.ddl"),
        "Generated DDL did not match expected for evolution enabled");

    String dml = HiveAvroORCQueryGenerator
        .generateTableMappingDML(inputSchema, outputSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.dml"),
        "Generated DML did not match expected for evolution enabled");
  }

  @Test
  public void testEvolutionDisabledForExistingTable() throws IOException {
    boolean isEvolutionEnabled = false;
    Optional<Table> destinationTableMeta = createEvolvedDestinationTable(schemaName, "default", "", true);

    String ddl = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(outputSchema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            null, isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_disabled.ddl"),
        "Generated DDL did not match expected for evolution disabled");

    String dml = HiveAvroORCQueryGenerator
        .generateTableMappingDML(inputSchema, outputSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_disabled.dml"),
        "Generated DML did not match expected for evolution disabled");
  }

  @Test
  public void testEvolutionDisabledForNewTable() throws IOException {
    boolean isEvolutionEnabled = false;
    Optional<Table> destinationTableMeta = Optional.absent();

    String ddl = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(outputSchema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            null, isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.ddl"),
        "Generated DDL did not match expected for evolution disabled");

    String dml = HiveAvroORCQueryGenerator
        .generateTableMappingDML(inputSchema, outputSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.dml"),
        "Generated DML did not match expected for evolution disabled");
  }

  @Test
  public void testLineageMissing() throws IOException {
    boolean isEvolutionEnabled = false;
    Optional<Table> destinationTableMeta = createEvolvedDestinationTable(schemaName, "default", "", false);

    String ddl = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(outputSchema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
            null, isEvolutionEnabled, destinationTableMeta,
            new HashMap<String, String>());

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_lineage_missing.ddl"),
        "Generated DDL did not match expected for evolution disabled");

    String dml = HiveAvroORCQueryGenerator
        .generateTableMappingDML(inputSchema, outputSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.<Map<String, String>>absent(), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_lineage_missing.dml"),
        "Generated DML did not match expected for evolution disabled");
  }

  @Test
  public void testEvolutionEnabledGenerateEvolutionDDL() {
    String orcStagingTableName = schemaName + "_staging";
    String orcTableName = schemaName;
    boolean isEvolutionEnabled = true;
    Optional<Table> destinationTableMeta = createEvolvedDestinationTable(schemaName, "default", "", true);
    Map<String, String> hiveColumns = new HashMap<>();

    // Call to help populate hiveColumns via real code path
    HiveAvroORCQueryGenerator.generateCreateTableDDL(outputSchema, schemaName, "/tmp/dummy", Optional.<String>absent(),
        Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
        Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
        Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
        null, isEvolutionEnabled, destinationTableMeta, hiveColumns);

    // Destination table exists
    List<String> generateEvolutionDDL = HiveAvroORCQueryGenerator
        .generateEvolutionDDL(orcStagingTableName, orcTableName, Optional.of(hiveDbName), Optional.of(hiveDbName),
            outputSchema, isEvolutionEnabled, hiveColumns, destinationTableMeta);
    Assert.assertEquals(generateEvolutionDDL.size(), 2);
    Assert.assertEquals(generateEvolutionDDL.get(1),
        "ALTER TABLE `sourceSchema` ADD COLUMNS (parentFieldRecord__nestedFieldInt int "
            + "COMMENT 'from flatten_source parentFieldRecord.nestedFieldInt')",
        "Generated evolution DDL did not match for evolution enabled");

    // Destination table does not exists
    destinationTableMeta = Optional.absent();
    generateEvolutionDDL = HiveAvroORCQueryGenerator
        .generateEvolutionDDL(orcStagingTableName, orcTableName, Optional.of(hiveDbName), Optional.of(hiveDbName),
            outputSchema, isEvolutionEnabled, hiveColumns, destinationTableMeta);
    // No DDL should be generated, because create table will take care of destination table
    Assert.assertEquals(generateEvolutionDDL.size(), 0,
        "Generated evolution DDL did not match for evolution enabled");
  }

  @Test
  public void testEvolutionDisabledGenerateEvolutionDDL() {
    String orcStagingTableName = schemaName + "_staging";
    String orcTableName = schemaName;
    boolean isEvolutionEnabled = false;
    Optional<Table> destinationTableMeta = createEvolvedDestinationTable(schemaName, "default", "", true);
    Map<String, String> hiveColumns = new HashMap<>();

    // Call to help populate hiveColumns via real code path
    HiveAvroORCQueryGenerator.generateCreateTableDDL(outputSchema, schemaName, "/tmp/dummy", Optional.<String>absent(),
        Optional.<Map<String, String>>absent(), Optional.<List<String>>absent(),
        Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
        Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(),
        null, isEvolutionEnabled, destinationTableMeta, hiveColumns);

    // Destination table exists
    List<String> generateEvolutionDDL = HiveAvroORCQueryGenerator
        .generateEvolutionDDL(orcStagingTableName, orcTableName, Optional.of(hiveDbName), Optional.of(hiveDbName),
            outputSchema, isEvolutionEnabled, hiveColumns, destinationTableMeta);
    // No DDL should be generated, because select based on destination table will selectively project columns
    Assert.assertEquals(generateEvolutionDDL.size(), 0,
        "Generated evolution DDL did not match for evolution disabled");

    // Destination table does not exists
    destinationTableMeta = Optional.absent();
    generateEvolutionDDL = HiveAvroORCQueryGenerator
        .generateEvolutionDDL(orcStagingTableName, orcTableName, Optional.of(hiveDbName), Optional.of(hiveDbName),
            outputSchema, isEvolutionEnabled, hiveColumns, destinationTableMeta);
    // No DDL should be generated, because create table will take care of destination table
    Assert.assertEquals(generateEvolutionDDL.size(), 0,
        "Generated evolution DDL did not match for evolution disabled");
  }

  private Optional<Table> createEvolvedDestinationTable(String tableName, String dbName, String location,
      boolean withComment) {
    List<FieldSchema> cols = new ArrayList<>();
    // Existing columns that match avroToOrcSchemaEvolutionTest/source_schema_evolution_enabled.ddl
    cols.add(new FieldSchema("parentFieldRecord__nestedFieldRecord__superNestedFieldString", "string",
        withComment ? "from flatten_source parentFieldRecord.nestedFieldRecord.superNestedFieldString" : ""));
    cols.add(new FieldSchema("parentFieldRecord__nestedFieldRecord__superNestedFieldInt", "int",
        withComment ? "from flatten_source parentFieldRecord.nestedFieldRecord.superNestedFieldInt" : ""));
    cols.add(new FieldSchema("parentFieldRecord__nestedFieldString", "string",
        withComment ? "from flatten_source parentFieldRecord.nestedFieldString" : ""));
    // The following column is skipped (simulating un-evolved schema):
    // Column name   : parentFieldRecord__nestedFieldInt
    // Column type   : int
    // Column comment: from flatten_source parentFieldRecord.nestedFieldInt
    cols.add(new FieldSchema("parentFieldInt", "int",
        withComment ? "from flatten_source parentFieldInt" : ""));
    // Extra schema
    cols.add(new FieldSchema("parentFieldRecord__nestedFieldString2", "string",
        withComment ? "from flatten_source parentFieldRecord.nestedFieldString2" : ""));

    String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    StorageDescriptor storageDescriptor = new StorageDescriptor(cols, location, inputFormat, outputFormat, false, 0,
        new SerDeInfo(), null, Lists.<Order>newArrayList(), null);
    Table table = new Table(tableName, dbName, "ketl_dev", 0, 0, 0, storageDescriptor,
        Lists.<FieldSchema>newArrayList(), Maps.<String,String>newHashMap(), "", "", "");

    return Optional.of(table);
  }
}
