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
import java.util.ArrayList;
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

import com.beust.jcommander.internal.Lists;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import gobblin.data.management.ConversionHiveTestUtils;
import gobblin.data.management.conversion.hive.util.HiveAvroORCQueryUtils;
import gobblin.util.AvroFlattener;


/**
 * Test for schema evolution enabled or disabled
 */
@Slf4j
@Test(groups = { "gobblin.data.management.conversion" })
public class HiveSchemaEvolutionTest {
  private static String resourceDir = "avroToOrcSchemaEvolutionTest";
  private static String schemaName = "sourceSchema";
  private static AvroFlattener avroFlattener = new AvroFlattener();
  private static Schema inputSchema;
  private static Schema outputSchema;

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

    String ddl = HiveAvroORCQueryUtils
        .generateCreateTableDDL(outputSchema,
            schemaName,
            "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryUtils.COLUMN_SORT_ORDER>>absent(),
            Optional.<Integer>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            isEvolutionEnabled,
            destinationTableMeta);

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir, 
        "source_schema_evolution_enabled.ddl"));

    String dml = HiveAvroORCQueryUtils.generateTableMappingDML(inputSchema,
        outputSchema,
        schemaName,
        schemaName + "_orc",
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<Map<String, String>>absent(),
        Optional.<Boolean>absent(),
        Optional.<Boolean>absent(),
        isEvolutionEnabled,
        destinationTableMeta);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.dml"));
  }

  @Test
  public void testEvolutionEnabledForNewTable() throws IOException {
    boolean isEvolutionEnabled = true;
    Optional<Table> destinationTableMeta = Optional.absent();

    String ddl = HiveAvroORCQueryUtils
        .generateCreateTableDDL(outputSchema,
            schemaName,
            "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryUtils.COLUMN_SORT_ORDER>>absent(),
            Optional.<Integer>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            isEvolutionEnabled,
            destinationTableMeta);

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir, 
        "source_schema_evolution_enabled.ddl"));

    String dml = HiveAvroORCQueryUtils.generateTableMappingDML(inputSchema,
        outputSchema,
        schemaName,
        schemaName + "_orc",
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<Map<String, String>>absent(),
        Optional.<Boolean>absent(),
        Optional.<Boolean>absent(),
        isEvolutionEnabled,
        destinationTableMeta);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.dml"));
  }

  @Test
  public void testEvolutionDisabledForExistingTable() throws IOException {
    boolean isEvolutionEnabled = false;
    Optional<Table> destinationTableMeta = createEvolvedDestinationTable(schemaName, "default", "", true);

    String ddl = HiveAvroORCQueryUtils
        .generateCreateTableDDL(outputSchema,
            schemaName,
            "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryUtils.COLUMN_SORT_ORDER>>absent(),
            Optional.<Integer>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            isEvolutionEnabled,
            destinationTableMeta);

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir, 
        "source_schema_evolution_disabled.ddl"));

    String dml = HiveAvroORCQueryUtils.generateTableMappingDML(inputSchema,
        outputSchema,
        schemaName,
        schemaName + "_orc",
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<Map<String, String>>absent(),
        Optional.<Boolean>absent(),
        Optional.<Boolean>absent(),
        isEvolutionEnabled,
        destinationTableMeta);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_disabled.dml"));
  }

  @Test
  public void testEvolutionDisabledForNewTable() throws IOException {
    boolean isEvolutionEnabled = false;
    Optional<Table> destinationTableMeta = Optional.absent();

    String ddl = HiveAvroORCQueryUtils
        .generateCreateTableDDL(outputSchema,
            schemaName,
            "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryUtils.COLUMN_SORT_ORDER>>absent(),
            Optional.<Integer>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            isEvolutionEnabled,
            destinationTableMeta);

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.ddl"));

    String dml = HiveAvroORCQueryUtils.generateTableMappingDML(inputSchema,
        outputSchema,
        schemaName,
        schemaName + "_orc",
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<Map<String, String>>absent(),
        Optional.<Boolean>absent(),
        Optional.<Boolean>absent(),
        isEvolutionEnabled,
        destinationTableMeta);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_evolution_enabled.dml"));
  }

  @Test
  public void testLineageMissing() throws IOException {
    boolean isEvolutionEnabled = false;
    Optional<Table> destinationTableMeta = createEvolvedDestinationTable(schemaName, "default", "", false);

    String ddl = HiveAvroORCQueryUtils
        .generateCreateTableDDL(outputSchema,
            schemaName,
            "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryUtils.COLUMN_SORT_ORDER>>absent(),
            Optional.<Integer>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<String>absent(),
            Optional.<Map<String, String>>absent(),
            isEvolutionEnabled,
            destinationTableMeta);

    Assert.assertEquals(ddl, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_lineage_missing.ddl"));

    String dml = HiveAvroORCQueryUtils.generateTableMappingDML(inputSchema,
        outputSchema,
        schemaName,
        schemaName + "_orc",
        Optional.<String>absent(),
        Optional.<String>absent(),
        Optional.<Map<String, String>>absent(),
        Optional.<Boolean>absent(),
        Optional.<Boolean>absent(),
        isEvolutionEnabled,
        destinationTableMeta);

    Assert.assertEquals(dml, ConversionHiveTestUtils.readQueryFromFile(resourceDir,
        "source_schema_lineage_missing.dml"));
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
    // cols.add(new FieldSchema("parentFieldRecord__nestedFieldInt", "int",
    //   withComment ? "from flatten_source parentFieldRecord.nestedFieldInt" : ""));
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
