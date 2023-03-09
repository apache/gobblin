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

package org.apache.gobblin.iceberg.predicates;

import com.google.common.io.Files;
import java.io.File;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.Dataset;
import org.apache.gobblin.dataset.test.SimpleDatasetForTesting;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Slf4j
@Test(dependsOnGroups = "icebergMetadataWriterTest")
public class DatasetHiveSchemaContainsNonOptionalUnionTest extends HiveMetastoreTest {

  private static String dbName = "dbname_" +
      DatasetHiveSchemaContainsNonOptionalUnionTest.class.getSimpleName().toLowerCase();
  private static File tmpDir;
  private static State state;
  private static String dbUri;
  private static String testTable = "test_table";

  @AfterSuite
  public void clean() throws Exception {
    FileUtils.forceDeleteOnExit(tmpDir);
  }

  @BeforeSuite
  public void setup() throws Exception {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
    startMetastore();
    tmpDir = Files.createTempDir();
    dbUri = String.format("%s/%s/%s", tmpDir.getAbsolutePath(),"metastore", dbName);
    try {
      metastoreClient.getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      metastoreClient.createDatabase(
          new Database(dbName, "database", dbUri, Collections.emptyMap()));
    }

    final State serdeProps = new State();
    final String avroSchema = "{\"type\": \"record\", \"name\": \"TestEvent\",\"namespace\": \"test.namespace\", \"fields\": "
        + "[{\"name\":\"fieldName\", \"type\": %s}]}";
    serdeProps.setProp("avro.schema.literal", String.format(avroSchema, "[\"string\", \"int\"]"));
    HiveTable testTable = createTestHiveTable_Avro(serdeProps);
    metastoreClient.createTable(HiveMetaStoreUtils.getTable(testTable));

    state = ConfigUtils.configToState(ConfigUtils.propertiesToConfig(hiveConf.getAllProperties()));
    state.setProp(DatasetHiveSchemaContainsNonOptionalUnion.PATTERN, "/data/(\\w+)/(\\w+)");
    Assert.assertNotNull(metastoreClient.getTable(dbName, DatasetHiveSchemaContainsNonOptionalUnionTest.testTable));
  }

  @Test
  public void testContainsNonOptionalUnion() throws Exception {
    DatasetHiveSchemaContainsNonOptionalUnion predicate = new DatasetHiveSchemaContainsNonOptionalUnion(state.getProperties());
    Dataset dataset = new SimpleDatasetForTesting("/data/" + dbName + "/" + testTable);
    Assert.assertTrue(predicate.test(dataset));
  }

  private HiveTable createTestHiveTable_Avro(State props) {
    HiveTable.Builder builder = new HiveTable.Builder();
    HiveTable hiveTable = builder.withDbName(dbName).withTableName(testTable).withProps(props).build();
    hiveTable.setInputFormat(AvroContainerInputFormat.class.getName());
    hiveTable.setOutputFormat(AvroContainerOutputFormat.class.getName());
    hiveTable.setSerDeType(AvroSerDe.class.getName());

    // Serialize then deserialize as a way to quickly setup table object
    Table table = HiveMetaStoreUtils.getTable(hiveTable);
    return HiveMetaStoreUtils.getHiveTable(table);
  }
}
