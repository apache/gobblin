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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveTable;


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
}
