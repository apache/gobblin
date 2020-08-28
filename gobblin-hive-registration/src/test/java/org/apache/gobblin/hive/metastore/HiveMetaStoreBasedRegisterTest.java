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

import com.google.common.base.Optional;
import java.io.IOException;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.spec.SimpleHiveSpec;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.util.AvroUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HiveMetaStoreBasedRegisterTest {
  @Test
  public void testUpdateSchemaMethod() throws IOException {

    final String databaseName = "testdb";
    final String tableName = "testtable";

    State state = new State();
    state.setProp(HiveMetaStoreBasedRegister.FETCH_LATEST_SCHEMA, true);
    state.setProp(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS, MockSchemaRegistry.class.getName());
    HiveMetaStoreBasedRegister register = new HiveMetaStoreBasedRegister(state, Optional.absent());
    Schema writerSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"TestEvent\","
        + " \"namespace\": \"test.namespace\", \"fields\": [{\"name\":\"testName\"," + " \"type\": \"int\"}]}");
    AvroUtils.setSchemaCreationTime(writerSchema, "111");

    //Build hiveTable
    HiveTable.Builder builder = new HiveTable.Builder();
    builder.withDbName(databaseName).withTableName(tableName);

    State serdeProps = new State();
    serdeProps.setProp("avro.schema.literal", writerSchema.toString());
    builder.withSerdeProps(serdeProps);

    HiveTable hiveTable = builder.build();
    HiveTable existingTable = builder.build();

    hiveTable.setInputFormat(AvroContainerInputFormat.class.getName());
    hiveTable.setOutputFormat(AvroContainerOutputFormat.class.getName());
    hiveTable.setSerDeType(AvroSerDe.class.getName());

    existingTable.setInputFormat(AvroContainerInputFormat.class.getName());
    existingTable.setOutputFormat(AvroContainerOutputFormat.class.getName());
    existingTable.setSerDeType(AvroSerDe.class.getName());

    SimpleHiveSpec.Builder specBuilder = new SimpleHiveSpec.Builder(new Path("pathString"))
        .withPartition(Optional.absent())
        .withTable(hiveTable);
    Table table = HiveMetaStoreUtils.getTable(hiveTable);
    SimpleHiveSpec simpleHiveSpec = specBuilder.build();

    //Test new schema equals existing schema, we don't change anything
    register.updateSchema(simpleHiveSpec, table, existingTable);
    Assert.assertEquals(table.getSd().getSerdeInfo().getParameters()
        .get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()), writerSchema.toString());

    //Test new schema does not equal to existing schema, and latest schema does not equals to existing schema
    //We set schema to writer schema
    register.schemaRegistry.get().register(writerSchema, "writerSchema");
    Schema existingSchema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"TestEvent_1\","
        + " \"namespace\": \"test.namespace\", \"fields\": [{\"name\":\"testName_1\"," + " \"type\": \"double\"}]}");
    AvroUtils.setSchemaCreationTime(existingSchema, "110");
    existingTable.getSerDeProps()
        .setProp(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), existingSchema.toString());
    register.updateSchema(simpleHiveSpec, table, existingTable);
    Assert.assertEquals(table.getSd().getSerdeInfo().getParameters()
        .get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()), writerSchema.toString());

    //Test new schema does not equal to existing schema, latest schema equals to existing schema,
    //in this case, table schema should be existingSchema
    register.schemaRegistry.get().register(existingSchema, "existingSchema");
    register.updateSchema(simpleHiveSpec, table, existingTable);
    Assert.assertEquals(table.getSd().getSerdeInfo().getParameters()
        .get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName()), existingSchema.toString());

  }

  public static class MockSchemaRegistry extends KafkaSchemaRegistry<String, Schema> {
    static Schema latestSchema = Schema.create(Schema.Type.STRING);

    public MockSchemaRegistry(Properties props) {
      super(props);
    }

    @Override
    protected Schema fetchSchemaByKey(String key) throws SchemaRegistryException {
      return null;
    }

    @Override
    public Schema getLatestSchemaByTopic(String topic) throws SchemaRegistryException {
      return latestSchema;
    }

    @Override
    public String register(Schema schema) throws SchemaRegistryException {
      return null;
    }

    @Override
    public String register(Schema schema, String name) throws SchemaRegistryException {
      this.latestSchema = schema;
      return schema.toString();
    }
  }
}
