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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;

@Test(singleThreaded = true)
public class HiveOrcSerDeManagerTest {
  private static String TEST_DB = "testDB";
  private static String TEST_TABLE = "testTable";
  private Path testBasePath;

  @BeforeClass
  public void setUp() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    this.testBasePath = new Path("orctestdir");
    fs.delete(this.testBasePath, true);
    fs.mkdirs(this.testBasePath);

    Files.copy(this.getClass().getResourceAsStream("/orc_input/input.orc"),
        Paths.get(this.testBasePath.toString(), "prefix-hive-test.orc"), StandardCopyOption.REPLACE_EXISTING);
    Files.copy(this.getClass().getResourceAsStream("/avro_input/input.avro"),
        Paths.get(this.testBasePath.toString(), "hive-test.notOrc"), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Test that the schema is written to the schema literal
   */
  @Test
  public void testOrcSchemaLiteral() throws IOException {
    State state = new State();
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testBasePath, registrationUnit);

    Assert.assertTrue(registrationUnit.getSerDeProps().getProp(HiveOrcSerDeManager.SCHEMA_LITERAL).contains(
        "name:string,timestamp:bigint"));
  }

  /**
   * Test empty extension
   */
  @Test
  public void testEmptyExtension() throws IOException {
    State state = new State();
    state.setProp(HiveOrcSerDeManager.FILE_EXTENSIONS_KEY, ",");
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testBasePath, registrationUnit);

    Assert.assertTrue(registrationUnit.getSerDeProps().getProp(HiveOrcSerDeManager.SCHEMA_LITERAL).contains(
        "name:string,timestamp:bigint"));
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

    manager.addSerDeProperties(this.testBasePath, registrationUnit);

    Assert.assertTrue(registrationUnit.getSerDeProps().getProp(HiveOrcSerDeManager.SCHEMA_LITERAL).contains(
        "name:string,timestamp:bigint"));
    Assert.assertEquals(registrationUnit.getSerDeType().get(), OrcSerde.class.getName());
    Assert.assertEquals(registrationUnit.getInputFormat().get(), "customInputFormat");
    Assert.assertEquals(registrationUnit.getOutputFormat().get(), "customOutputFormat");
  }

  /**
   * Test that error is raised if no orc files found during schema retrieval
   */
  @Test(expectedExceptions = FileNotFoundException.class, expectedExceptionsMessageRegExp = "No files in Dataset:orctestdir found for schema retrieval")
  public void testNoOrcFiles() throws IOException {
    State state = new State();
    state.setProp(HiveOrcSerDeManager.FILE_EXTENSIONS_KEY, ".notOrc");
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testBasePath, registrationUnit);
  }

  /**
   * Test prefix filter
   */
  @Test(expectedExceptions = FileNotFoundException.class, expectedExceptionsMessageRegExp = "No files in Dataset:orctestdir found for schema retrieval")
  public void testPrefixFilter() throws IOException {
    State state = new State();
    state.setProp(HiveOrcSerDeManager.IGNORED_FILE_PREFIXES_KEY, "prefix-");
    HiveOrcSerDeManager manager = new HiveOrcSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testBasePath, registrationUnit);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.delete(this.testBasePath, true);
  }

}
