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

package org.apache.gobblin.hive.orc;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.binary_creation.AvroTestTools;
import org.apache.gobblin.binary_creation.OrcTestTools;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;
import org.apache.gobblin.util.HadoopUtils;

import static org.apache.gobblin.hive.orc.HiveOrcSerDeManager.ENABLED_ORC_TYPE_CHECK;


@Test(singleThreaded = true)
public class HiveOrcSerDeManagerTest {
  private static String TEST_DB = "testDB";
  private static String TEST_TABLE = "testTable";
  private Path testBasePath;
  private Path testRegisterPath;

  @BeforeClass
  public void setUp() throws IOException, SerDeException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    this.testBasePath = new Path("orctestdir");
    this.testRegisterPath = new Path(this.testBasePath, "register");
    fs.delete(this.testBasePath, true);
    fs.mkdirs(this.testRegisterPath);

    OrcTestTools orcTestTools = new OrcTestTools();

    orcTestTools.writeJsonResourceRecordsAsBinary("avro_input", fs, this.testBasePath, null);
    HadoopUtils.copyFile(fs, new Path(this.testBasePath, "input"), fs, new Path(this.testRegisterPath,
        "prefix-hive-test.orc"), true, new Configuration());

    AvroTestTools avroTestTools = new AvroTestTools();

    avroTestTools.writeJsonResourceRecordsAsBinary("avro_input", fs, this.testBasePath, null);
    HadoopUtils.copyFile(fs, new Path(this.testBasePath, "input.avro"), fs, new Path(this.testRegisterPath, "hive-test.notOrc"), true, new Configuration());
  }

  /**
   * Test that the schema is written to the schema literal and attributes required for initializing orc serde object.
   */
  @Test
  public void testOrcSchemaLiteral() throws IOException {
    State state = new State();
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testRegisterPath, registrationUnit);


    List<String> columns = Arrays.asList(registrationUnit.getSerDeProps().getProp(serdeConstants.LIST_COLUMNS).split(","));
    Assert.assertTrue(columns.get(0).equals("name"));
    Assert.assertTrue(columns.get(1).equals("timestamp"));
    List<String> columnTypes = Arrays.asList(registrationUnit.getSerDeProps().getProp(serdeConstants.LIST_COLUMN_TYPES).split(","));
    Assert.assertTrue(columnTypes.get(0).equals("string"));
    Assert.assertTrue(columnTypes.get(1).equals("bigint"));
  }

  /**
   * Test empty extension
   */
  @Test
  public void testEmptyExtension() throws IOException {
    State state = new State();
    state.setProp(ENABLED_ORC_TYPE_CHECK, true);
    state.setProp(HiveOrcSerDeManager.FILE_EXTENSIONS_KEY, ",");
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testRegisterPath, registrationUnit);

    examineSchema(registrationUnit);
  }

  /**
   * Test custom serde config
   */
  @Test
  public void testCustomSerdeConfig() throws IOException {
    State state = new State();
    state.setProp(HiveOrcSerDeManager.SERDE_TYPE_KEY, OrcSerde.class.getName());
    state.setProp(HiveOrcSerDeManager.INPUT_FORMAT_CLASS_KEY, "customInputFormat");
    state.setProp(HiveOrcSerDeManager.OUTPUT_FORMAT_CLASS_KEY, "customOutputFormat");

    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testRegisterPath, registrationUnit);

    examineSchema(registrationUnit);
    Assert.assertEquals(registrationUnit.getSerDeType().get(), OrcSerde.class.getName());
    Assert.assertEquals(registrationUnit.getInputFormat().get(), "customInputFormat");
    Assert.assertEquals(registrationUnit.getOutputFormat().get(), "customOutputFormat");
  }

  /**
   * Test that error is raised if no orc files found during schema retrieval
   */
  @Test(expectedExceptions = FileNotFoundException.class, expectedExceptionsMessageRegExp = "No files in Dataset:orctestdir/register found for schema retrieval")
  public void testNoOrcFiles() throws IOException {
    State state = new State();
    state.setProp(ENABLED_ORC_TYPE_CHECK, true);
    state.setProp(HiveOrcSerDeManager.FILE_EXTENSIONS_KEY, ".notOrc");
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testRegisterPath, registrationUnit);
  }

  /**
   * Test prefix filter
   */
  @Test(expectedExceptions = FileNotFoundException.class, expectedExceptionsMessageRegExp = "No files in Dataset:orctestdir/register found for schema retrieval")
  public void testPrefixFilter() throws IOException {
    State state = new State();
    state.setProp(HiveOrcSerDeManager.IGNORED_FILE_PREFIXES_KEY, "prefix-");
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testRegisterPath, registrationUnit);
  }

  private void examineSchema(HiveRegistrationUnit registrationUnit) {
    List<String> columns = Arrays.asList(registrationUnit.getSerDeProps().getProp(serdeConstants.LIST_COLUMNS).split(","));
    Assert.assertTrue(columns.get(0).equals("name"));
    Assert.assertTrue(columns.get(1).equals("timestamp"));
    List<String> columnTypes = Arrays.asList(registrationUnit.getSerDeProps().getProp(serdeConstants.LIST_COLUMN_TYPES).split(","));
    Assert.assertTrue(columnTypes.get(0).equals("string"));
    Assert.assertTrue(columnTypes.get(1).equals("bigint"));
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.delete(this.testBasePath, true);
  }

}
