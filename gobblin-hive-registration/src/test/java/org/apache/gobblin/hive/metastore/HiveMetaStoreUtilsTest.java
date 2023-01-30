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

package org.apache.gobblin.hive.metastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveTable;

import static org.apache.gobblin.hive.metastore.HiveMetaStoreUtils.getParameters;


public class HiveMetaStoreUtilsTest {
  @Test
  public void testGetTableAvro() {
    final String databaseName = "testdb";
    final String tableName = "testtable";

    HiveTable.Builder builder = new HiveTable.Builder();

    builder.withDbName(databaseName).withTableName(tableName);

    State serdeProps = new State();
    serdeProps.setProp("avro.schema.literal", "{\"type\": \"record\", \"name\": \"TestEvent\","
        + " \"namespace\": \"test.namespace\", \"fields\": [{\"name\":\"a\"," + " \"type\": \"int\"}]}");
    builder.withSerdeProps(serdeProps);

    HiveTable hiveTable = builder.build();
    hiveTable.setInputFormat(AvroContainerInputFormat.class.getName());
    hiveTable.setOutputFormat(AvroContainerOutputFormat.class.getName());
    hiveTable.setSerDeType(AvroSerDe.class.getName());

    Table table = HiveMetaStoreUtils.getTable(hiveTable);
    Assert.assertEquals(table.getDbName(), databaseName);
    Assert.assertEquals(table.getTableName(), tableName);

    StorageDescriptor sd = table.getSd();
    Assert.assertEquals(sd.getInputFormat(), AvroContainerInputFormat.class.getName());
    Assert.assertEquals(sd.getOutputFormat(), AvroContainerOutputFormat.class.getName());
    Assert.assertNotNull(sd.getSerdeInfo());
    Assert.assertEquals(sd.getSerdeInfo().getSerializationLib(), AvroSerDe.class.getName());

    List<FieldSchema> fields = sd.getCols();
    Assert.assertTrue(fields != null && fields.size() == 1);
    FieldSchema fieldA = fields.get(0);
    Assert.assertEquals(fieldA.getName(), "a");
    Assert.assertEquals(fieldA.getType(), "int");
  }

  @Test
  public void testGetTableOrc() {
    final String databaseName = "db";
    final String tableName = "tbl";
    HiveTable.Builder builder = new HiveTable.Builder();
    builder.withDbName(databaseName).withTableName(tableName);

    HiveTable hiveTable = builder.build();

    // SerDe props are
    State serdeProps = new State();
    serdeProps.setProp("columns", "timestamp,namespace,name,metadata");
    serdeProps.setProp("columns.types", "bigint,string,string,map<string,string>");

    hiveTable.getProps().addAll(serdeProps);

    hiveTable.setInputFormat(OrcInputFormat.class.getName());
    hiveTable.setOutputFormat(OrcOutputFormat.class.getName());
    hiveTable.setSerDeType(OrcSerde.class.getName());

    Table table = HiveMetaStoreUtils.getTable(hiveTable);
    Assert.assertEquals(table.getDbName(), databaseName);
    Assert.assertEquals(table.getTableName(), tableName);

    StorageDescriptor sd = table.getSd();
    Assert.assertEquals(sd.getInputFormat(), OrcInputFormat.class.getName());
    Assert.assertEquals(sd.getOutputFormat(), OrcOutputFormat.class.getName());
    Assert.assertNotNull(sd.getSerdeInfo());
    Assert.assertEquals(sd.getSerdeInfo().getSerializationLib(), OrcSerde.class.getName());

    // verify column name
    List<FieldSchema> fields = sd.getCols();
    Assert.assertTrue(fields != null && fields.size() == 4);
    FieldSchema fieldA = fields.get(0);
    Assert.assertEquals(fieldA.getName(), "timestamp");
    Assert.assertEquals(fieldA.getType(), "bigint");

    FieldSchema fieldB = fields.get(1);
    Assert.assertEquals(fieldB.getName(), "namespace");
    Assert.assertEquals(fieldB.getType(), "string");

    FieldSchema fieldC = fields.get(2);
    Assert.assertEquals(fieldC.getName(), "name");
    Assert.assertEquals(fieldC.getType(), "string");


    FieldSchema fieldD = fields.get(3);
    Assert.assertEquals(fieldD.getName(), "metadata");
    Assert.assertEquals(fieldD.getType(), "map<string,string>");
  }

  @Test
  public void testGetTableAvroInvalidSchema() {
    final String databaseName = "testdb";
    final String tableName = "testtable";

    HiveTable.Builder builder = new HiveTable.Builder();

    builder.withDbName(databaseName).withTableName(tableName);

    State serdeProps = new State();
    serdeProps.setProp("avro.schema.literal", "invalid schema");
    builder.withSerdeProps(serdeProps);

    HiveTable hiveTable = builder.build();
    hiveTable.setInputFormat(AvroContainerInputFormat.class.getName());
    hiveTable.setOutputFormat(AvroContainerOutputFormat.class.getName());
    hiveTable.setSerDeType(AvroSerDe.class.getName());

    Table table = HiveMetaStoreUtils.getTable(hiveTable);
    Assert.assertEquals(table.getDbName(), databaseName);
    Assert.assertEquals(table.getTableName(), tableName);

    StorageDescriptor sd = table.getSd();
    Assert.assertEquals(sd.getInputFormat(), AvroContainerInputFormat.class.getName());
    Assert.assertEquals(sd.getOutputFormat(), AvroContainerOutputFormat.class.getName());
    Assert.assertNotNull(sd.getSerdeInfo());
    Assert.assertEquals(sd.getSerdeInfo().getSerializationLib(), AvroSerDe.class.getName());

    List<FieldSchema> fields = sd.getCols();
    Assert.assertTrue(fields != null && fields.size() == 0);
  }

  @Test
  public void testInVokeDetermineSchemaOrThrowExceptionMethod() {
    try {
      HiveMetaStoreUtils.inVokeDetermineSchemaOrThrowExceptionMethod(new Properties(), new Configuration());
    } catch (Exception e) {
      Assert.assertFalse(e instanceof NoSuchMethodException);
    }
  }

  @Test
  public void testGetHiveTable() throws Exception {
    final String databaseName = "testdb";
    final String tableName = "testtable";
    final String tableSdLoc = "/tmp/testtable";
    final String partitionName = "partitionName";

    State serdeProps = new State();
    serdeProps.setProp("avro.schema.literal", "{\"type\": \"record\", \"name\": \"TestEvent\","
            + " \"namespace\": \"test.namespace\", \"fields\": [{\"name\":\"testName\"," + " \"type\": \"int\"}]}");

    List<FieldSchema> fieldSchemas = new ArrayList<>();
    fieldSchemas.add(new FieldSchema("testName","int", "testContent"));
    SerDeInfo si = new SerDeInfo();
    si.setParameters(getParameters(serdeProps));
    si.setName(tableName);

    StorageDescriptor sd = new StorageDescriptor(fieldSchemas, tableSdLoc,
            AvroContainerInputFormat.class.getName(), AvroContainerOutputFormat.class.getName(),
            false, 0, si, null, Lists.<Order>newArrayList(), null);
    sd.setParameters(getParameters(serdeProps));
    Table table = new Table(tableName, databaseName, "testOwner", 0, 0, 0, sd,
            Lists.<FieldSchema>newArrayList(), Maps.<String,String>newHashMap(), "", "", "");
    table.addToPartitionKeys(new FieldSchema(partitionName, "string", "some comment"));

    HiveTable hiveTable = HiveMetaStoreUtils.getHiveTable(table);
    Assert.assertEquals(hiveTable.getDbName(), databaseName);
    Assert.assertEquals(hiveTable.getTableName(), tableName);

    Assert.assertTrue(hiveTable.getInputFormat().isPresent());
    Assert.assertTrue(hiveTable.getOutputFormat().isPresent());
    Assert.assertEquals(hiveTable.getInputFormat().get(), AvroContainerInputFormat.class.getName());
    Assert.assertEquals(hiveTable.getOutputFormat().get(), AvroContainerOutputFormat.class.getName());
    Assert.assertNotNull(hiveTable.getSerDeType());

    List<HiveRegistrationUnit.Column> fields = hiveTable.getColumns();
    Assert.assertTrue(fields != null && fields.size() == 1);
    HiveRegistrationUnit.Column fieldA = fields.get(0);
    Assert.assertEquals(fieldA.getName(), "testName");
    Assert.assertEquals(fieldA.getType(), "int");

  }

  @Test
  public void testContainsUnionType_AvroSucceeds() {
    final State serdeProps = new State();
    final String avroSchema = "{\"type\": \"record\", \"name\": \"TestEvent\",\"namespace\": \"test.namespace\", \"fields\": [{\"name\":\"fieldName\", \"type\": %s}]}";
    Consumer<String> assertContainsNonOptionalUnionType = fieldType -> {
      serdeProps.setProp("avro.schema.literal", String.format(avroSchema, fieldType));
      HiveTable hiveTable = createTestHiveTable_Avro(serdeProps);
      Assert.assertEquals(hiveTable.getColumns().size(), 1);
      Assert.assertTrue(HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(hiveTable));
    };

    assertContainsNonOptionalUnionType.accept("[\"string\", \"int\"]");
    assertContainsNonOptionalUnionType.accept("[\"string\", \"int\", \"null\"]");
    assertContainsNonOptionalUnionType.accept("[{\"type\":\"map\",\"values\":[\"boolean\",\"null\", {\"type\": \"array\", \"items\":\"string\"}]},\"null\"]");
  }

  @Test
  public void testContainsUnionType_AvroFails() {
    final State serdeProps = new State();
    serdeProps.setProp("avro.schema.literal", "{\"type\": \"record\", \"name\": \"TestEvent\",\"namespace\": \"test.namespace\", "
        + "\"fields\": ["
            + "{\"name\":\"someString\", \"type\": \"string\"}, "
            + "{\"name\":\"aNullableInt\", \"type\": [\"null\", \"int\"]},"
            + "{\"name\":\"nonNullableInt\", \"type\": [\"int\"]},"
            + "{\"name\":\"nonArray\", \"type\": [{\"type\": \"array\", \"items\":{\"type\":\"map\",\"values\":\"string\"}}]}"
        + "]}");

    HiveTable hiveTable = createTestHiveTable_Avro(serdeProps);
    Assert.assertEquals(hiveTable.getColumns().size(), 4);

    Assert.assertFalse(HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(hiveTable));
  }

  @Test
  public void testContainsUnionType_AvroNoSchemaLiteral() {
    HiveTable table = new HiveTable.Builder().withDbName("db").withTableName("tb").build();
    Assert.assertThrows(RuntimeException.class, () -> HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(table));
  }

  @Test
  public void testContainsUnionType_OrcUnionType() {
    final State serdeProps = new State();
    serdeProps.setProp("columns", "someInt,someString,someMap,someUT");
    // NOTE: unlike in avro, all values in ORC are nullable, so it's not necessary to test null permutations
    serdeProps.setProp("columns.types", "bigint,string,map<string,string>,uniontype<string,int>");

    HiveTable hiveTable = createTestHiveTable_ORC(serdeProps);
    Assert.assertEquals(hiveTable.getColumns().size(), 4);

    Assert.assertTrue(HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(hiveTable));
  }

  @Test
  public void testContainsUnionType_OrcNestedValue() {
    final State serdeProps = new State();
    serdeProps.setProp("columns", "nestedNonOptionalUT");
    serdeProps.setProp("columns.types", "map<string,array<struct<i:int,someUT:uniontype<array<string>,struct<i:int>>>>>");

    HiveTable hiveTable = createTestHiveTable_ORC(serdeProps);
    Assert.assertEquals(hiveTable.getColumns().size(), 1);

    Assert.assertTrue(HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(hiveTable));
  }

  @Test
  public void testContainsUnionType_OrcNestedUnionPrimitive() {
    final State serdeProps = new State();
    serdeProps.setProp("columns", "nesteduniontypeint");
    serdeProps.setProp("columns.types", "uniontype<array<map<string,struct<i:int,someUt:uniontype<int>>>>>");

    HiveTable hiveTable = createTestHiveTable_ORC(serdeProps);
    Assert.assertEquals(hiveTable.getColumns().size(), 1);

    Assert.assertFalse(HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(hiveTable));
  }

  @Test
  public void testContainsUnionType_OrcPrimitive() {
    final State serdeProps = new State();
    serdeProps.setProp("columns", "timestamp,uniontypeint");
    serdeProps.setProp("columns.types", "bigint,uniontype<int>");

    HiveTable hiveTable = createTestHiveTable_ORC(serdeProps);
    Assert.assertEquals(hiveTable.getColumns().size(), 2);

    Assert.assertFalse(HiveMetaStoreUtils.containsNonOptionalUnionTypeColumn(hiveTable));
  }

  private HiveTable createTestHiveTable_ORC(State props) {
    return createTestHiveTable("testDb", "testTable", props, (hiveTable) -> {
      hiveTable.setInputFormat(OrcInputFormat.class.getName());
      hiveTable.setOutputFormat(OrcOutputFormat.class.getName());
      hiveTable.setSerDeType(OrcSerde.class.getName());
      return null;
    });
  }

  private HiveTable createTestHiveTable_Avro(State props) {
    return createTestHiveTable("testDB", "testTable", props, (hiveTable) -> {
      hiveTable.setInputFormat(AvroContainerInputFormat.class.getName());
      hiveTable.setOutputFormat(AvroContainerOutputFormat.class.getName());
      hiveTable.setSerDeType(AvroSerDe.class.getName());
      return null;
    });
  }

  private HiveTable createTestHiveTable(String dbName, String tableName, State props, Function<HiveTable, Void> additionalSetup) {
    HiveTable.Builder builder = new HiveTable.Builder();
    HiveTable hiveTable = builder.withDbName(dbName).withTableName(tableName).withProps(props).build();
    additionalSetup.apply(hiveTable);

    // Serialize then deserialize as a way to quickly setup tables for other tests in util class
    Table table = HiveMetaStoreUtils.getTable(hiveTable);
    return HiveMetaStoreUtils.getHiveTable(table);
  }
}
