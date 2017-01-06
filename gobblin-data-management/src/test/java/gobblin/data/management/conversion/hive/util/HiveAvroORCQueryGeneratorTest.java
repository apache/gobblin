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
            null, isEvolutionEnabled, destinationTableMeta,
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
            null, isEvolutionEnabled, destinationTableMeta,
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
            null, isEvolutionEnabled, destinationTableMeta,
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
            null, isEvolutionEnabled, destinationTableMeta,
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
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(), null, isEvolutionEnabled,
            destinationTableMeta, new HashMap<String, String>());

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
   * Test Multi-partition DDL generation
   * @throws IOException
   */
  @Test
  public void testMultiPartitionDDL() throws IOException {
    String schemaName = "testMultiPartitionDDL";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir, "recordWithinRecordWithinRecord_nested.json");

    AvroFlattener avroFlattener = new AvroFlattener();
    Schema flattenedSchema = avroFlattener.flatten(schema, true);

    Map<String, String> partitionDDLInfo = ImmutableMap.of("datepartition", "string", "id", "int", "country", "string");

    String q = HiveAvroORCQueryGenerator
        .generateCreateTableDDL(flattenedSchema, schemaName, "file:/user/hive/warehouse/" + schemaName,
            Optional.<String>absent(), Optional.of(partitionDDLInfo), Optional.<List<String>>absent(),
            Optional.<Map<String, HiveAvroORCQueryGenerator.COLUMN_SORT_ORDER>>absent(), Optional.<Integer>absent(),
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(), null, isEvolutionEnabled,
            destinationTableMeta, new HashMap<String, String>());

    Assert.assertEquals(q,
        ConversionHiveTestUtils.readQueryFromFile(resourceDir, "testMultiPartition.ddl"));
  }

  /***
   * Test Multi-partition DML generation
   * @throws IOException
   */
  @Test
  public void testMultiPartitionDML() throws IOException {
    String schemaName = "testMultiPartitionDML";
    Schema schema = ConversionHiveTestUtils.readSchemaFromJsonFile(resourceDir,
        "recordWithinRecordWithinRecord_nested.json");

    AvroFlattener avroFlattener = new AvroFlattener();
    Schema flattenedSchema = avroFlattener.flatten(schema, true);

    Map<String, String> partitionDMLInfo = ImmutableMap.of("datepartition", "2016-01-01", "id", "101", "country", "US");

    String q = HiveAvroORCQueryGenerator
        .generateTableMappingDML(schema, flattenedSchema, schemaName, schemaName + "_orc", Optional.<String>absent(),
            Optional.<String>absent(), Optional.of(partitionDMLInfo), Optional.<Boolean>absent(),
            Optional.<Boolean>absent(), isEvolutionEnabled, destinationTableMeta, rowLimit);

    Assert.assertEquals(q.trim(), ConversionHiveTestUtils.readQueryFromFile(resourceDir, "testMultiPartition.dml"));
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
            Optional.<String>absent(), Optional.<String>absent(), Optional.<String>absent(), null, isEvolutionEnabled,
            destinationTableMeta, new HashMap<String, String>());
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

    Assert.assertEquals(q.trim(), ConversionHiveTestUtils.readQueryFromFile(resourceDir, "flattenedWithRowLimit.dml"));
  }

  @Test
  public void testDropPartitions() throws Exception {

    // Test multiple partition-spec drop method
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

    // Test single partition-spec drop method
    Map<String, String> partitionsDMLInfo = ImmutableMap.of("datepartition", "2016-01-01", "sizepartition", "10");
    ddl = HiveAvroORCQueryGenerator.generateDropPartitionsDDL("db1", "table1", partitionsDMLInfo);

    Assert.assertEquals(ddl.size(), 2);
    Assert.assertEquals(ddl.get(0), "USE db1\n");
    Assert.assertEquals(ddl.get(1),
        "ALTER TABLE table1 DROP IF EXISTS PARTITION (`datepartition`='2016-01-01', `sizepartition`='10') ");
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

  @Test
  public void testHiveTypeEscaping() throws Exception {
    String type = "array<struct<singleItems:array<struct<scoredEntity:struct<id:string,score:float,"
        + "sourceName:string,sourceModel:string>,scores:struct<fprScore:double,fprUtility:double,"
        + "calibratedFprUtility:double,sprScore:double,adjustedSprScore:double,sprUtility:double>,"
        + "sponsoredFlag:string,blendingRequestId:string,forExploration:boolean,d2Resource:string,"
        + "restliFinder:string,trackingId:binary,aggregation:struct<positionInAggregation:struct<index:int>,"
        + "typeOfAggregation:string>,decoratedFeedUpdateData:struct<avoData:struct<actorUrn:string,verbType:"
        + "string,objectUrn:string,objectType:string>,attributedActivityUrn:string,createdTime:bigint,totalLikes:"
        + "bigint,totalComments:bigint,rootActivity:struct<activityUrn:string,avoData:struct<actorUrn:string,"
        + "verbType:string,objectUrn:string,objectType:string>>>>>,scores:struct<fprScore:double,fprUtility:double,"
        + "calibratedFprUtility:double,sprScore:double,adjustedSprScore:double,sprUtility:double>,position:int>>";
    String expectedEscapedType = "array<struct<`singleItems`:array<struct<`scoredEntity`:struct<`id`:string,"
        + "`score`:float,`sourceName`:string,`sourceModel`:string>,`scores`:struct<`fprScore`:double,"
        + "`fprUtility`:double,`calibratedFprUtility`:double,`sprScore`:double,`adjustedSprScore`:double,"
        + "`sprUtility`:double>,`sponsoredFlag`:string,`blendingRequestId`:string,`forExploration`:boolean,"
        + "`d2Resource`:string,`restliFinder`:string,`trackingId`:binary,`aggregation`:struct<`positionInAggregation`"
        + ":struct<`index`:int>,`typeOfAggregation`:string>,`decoratedFeedUpdateData`:struct<`avoData`:"
        + "struct<`actorUrn`:string,`verbType`:string,`objectUrn`:string,`objectType`:string>,`attributedActivityUrn`"
        + ":string,`createdTime`:bigint,`totalLikes`:bigint,`totalComments`:bigint,`rootActivity`:struct<`activityUrn`"
        + ":string,`avoData`:struct<`actorUrn`:string,`verbType`:string,`objectUrn`:string,`objectType`:string>>>>>,"
        + "`scores`:struct<`fprScore`:double,`fprUtility`:double,`calibratedFprUtility`:double,`sprScore`:double,"
        + "`adjustedSprScore`:double,`sprUtility`:double>,`position`:int>>";
    String actualEscapedType = HiveAvroORCQueryGenerator.escapeHiveType(type);

    Assert.assertEquals(actualEscapedType, expectedEscapedType);
  }

  @Test
  public void testValidTypeEvolution() throws Exception {
    // Check a few evolved types
    Assert.assertTrue(HiveAvroORCQueryGenerator.isTypeEvolved("float", "int"));
    Assert.assertTrue(HiveAvroORCQueryGenerator.isTypeEvolved("double", "float"));
    Assert.assertTrue(HiveAvroORCQueryGenerator.isTypeEvolved("string", "varchar"));
    Assert.assertTrue(HiveAvroORCQueryGenerator.isTypeEvolved("double", "string"));

    // Check if type is same
    Assert.assertFalse(HiveAvroORCQueryGenerator.isTypeEvolved("int", "int"));
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testInvalidTypeEvolution() throws Exception {
    // Check for in-compatible types
    HiveAvroORCQueryGenerator.isTypeEvolved("boolean", "int");
  }

  @Test
  public void testCreateOrUpdateViewDDL() throws Exception {
    // Check if two queries for Create and Update View have been generated
    List<String> ddls = HiveAvroORCQueryGenerator.generateCreateOrUpdateViewDDL("db1", "tbl1", "db2" ,"view1", true);

    Assert.assertEquals(ddls.size(), 2, "Two queries for Create and Update should have been generated");
    Assert.assertEquals(ddls.get(0), "CREATE VIEW IF NOT EXISTS `db2`.`view1` AS SELECT * FROM `db1`.`tbl1`");
    Assert.assertEquals(ddls.get(1), "ALTER VIEW `db2`.`view1` AS SELECT * FROM `db1`.`tbl1`");

    // Check if two queries for Create and Update View have been generated
    ddls = HiveAvroORCQueryGenerator.generateCreateOrUpdateViewDDL("db1", "tbl1", "db2" ,"view1", false);

    Assert.assertEquals(ddls.size(), 1, "One query for Create only should have been generated");
    Assert.assertEquals(ddls.get(0), "CREATE VIEW IF NOT EXISTS `db2`.`view1` AS SELECT * FROM `db1`.`tbl1`");
  }

  @Test
  public void testAlterTableOrPartitionStorageFormatDDL() throws Exception {
    Map<String, String> partitions = ImmutableMap.of("datepartition", "2016-01-01", "sizepartition", "10");

    // For Partition
    List<String> ddls = HiveAvroORCQueryGenerator
        .generateAlterTableOrPartitionStorageFormatDDL("db1", "table1", Optional.of(partitions), "orc");
    Assert.assertEquals(ddls.size(), 2, "Two queries to set DB and Alter table should have been created");
    Assert.assertEquals(ddls.get(0), "USE db1\n");
    Assert.assertEquals(ddls.get(1), "ALTER TABLE table1 PARTITION (`datepartition`='2016-01-01', `sizepartition`='10')  SET FILEFORMAT orc");

    // For Table
    ddls = HiveAvroORCQueryGenerator
        .generateAlterTableOrPartitionStorageFormatDDL("db1", "table1", Optional.<Map<String,String>>absent(), "orc");
    Assert.assertEquals(ddls.size(), 2, "Two queries to set DB and Alter table should have been created");
    Assert.assertEquals(ddls.get(0), "USE db1\n");
    Assert.assertEquals(ddls.get(1), "ALTER TABLE table1  SET FILEFORMAT orc");
  }
 }
