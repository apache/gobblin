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

package org.apache.gobblin.hive.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.hive.HiveTable;


@Test(singleThreaded = true)
public class HiveAvroSerDeManagerTest {
  private static String TEST_DB = "testDB";
  private static String TEST_TABLE = "testTable";
  private Path testBasePath;

  @BeforeClass
  public void setUp() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    this.testBasePath = new Path("testdir");
    fs.delete(this.testBasePath, true);
    fs.delete(this.testBasePath, true);

    fs.mkdirs(this.testBasePath);

    Files.copy(this.getClass().getResourceAsStream("/test-hive-table/hive-test.avro"),
        Paths.get(this.testBasePath.toString(), "hive-test.avro"), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Test that the schema is written to the schema literal
   */
  @Test
  public void testSchemaLiteral() throws IOException {
    State state = new State();
    HiveAvroSerDeManager manager = new HiveAvroSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    manager.addSerDeProperties(this.testBasePath, registrationUnit);

    Assert.assertTrue(registrationUnit.getSerDeProps().getProp(HiveAvroSerDeManager.SCHEMA_LITERAL).contains("example.avro"));
  }

  @Test
  public void testSchemaUrl() throws IOException {
    State state = new State();
    state.setProp(HiveAvroSerDeManager.SCHEMA_LITERAL_LENGTH_LIMIT, "10");

    validateSchemaUrl(state, HiveAvroSerDeManager.DEFAULT_SCHEMA_FILE_NAME, false);
  }

  @Test
  public void testSchemaUrlWithExistingFile() throws IOException {
    State state = new State();
    state.setProp(HiveAvroSerDeManager.SCHEMA_LITERAL_LENGTH_LIMIT, "10");

    validateSchemaUrl(state, HiveAvroSerDeManager.DEFAULT_SCHEMA_FILE_NAME, true);
  }

  @Test
  public void testSchemaUrlWithTempFile() throws IOException {
    final String SCHEMA_FILE_NAME = "test_temp.avsc";
    State state = new State();
    state.setProp(HiveAvroSerDeManager.SCHEMA_LITERAL_LENGTH_LIMIT, "10");
    state.setProp(HiveAvroSerDeManager.USE_SCHEMA_TEMP_FILE, "true");
    state.setProp(HiveAvroSerDeManager.SCHEMA_FILE_NAME, SCHEMA_FILE_NAME);
    state.setProp(HiveAvroSerDeManager.USE_SCHEMA_TEMP_FILE, "true");

    validateSchemaUrl(state, SCHEMA_FILE_NAME, false);
  }

  @Test
  public void testSchemaUrlWithTempFileAndExistingFile() throws IOException {
    final String SCHEMA_FILE_NAME = "test_temp.avsc";
    State state = new State();
    state.setProp(HiveAvroSerDeManager.SCHEMA_LITERAL_LENGTH_LIMIT, "10");
    state.setProp(HiveAvroSerDeManager.USE_SCHEMA_TEMP_FILE, "true");
    state.setProp(HiveAvroSerDeManager.SCHEMA_FILE_NAME, SCHEMA_FILE_NAME);
    state.setProp(HiveAvroSerDeManager.USE_SCHEMA_TEMP_FILE, "true");

    validateSchemaUrl(state, SCHEMA_FILE_NAME, true);
  }

  private void validateSchemaUrl(State state, String targetSchemaFileName, boolean createConflictingFile) throws IOException {
    HiveAvroSerDeManager manager = new HiveAvroSerDeManager(state);
    HiveRegistrationUnit registrationUnit = (new HiveTable.Builder()).withDbName(TEST_DB).withTableName(TEST_TABLE).build();

    // Clean up existing file
    String targetPathStr = new Path(this.testBasePath, targetSchemaFileName).toString();
    File targetFile = new File(targetPathStr);
    targetFile.delete();

    // create a conflicting file
    if (createConflictingFile) {
      targetFile.createNewFile();
    }

    manager.addSerDeProperties(this.testBasePath, registrationUnit);

    Assert.assertNull(registrationUnit.getSerDeProps().getProp(HiveAvroSerDeManager.SCHEMA_LITERAL));
    String schemaUrl = registrationUnit.getSerDeProps().getProp(HiveAvroSerDeManager.SCHEMA_URL);
    Assert.assertEquals(schemaUrl, targetPathStr);
    Assert.assertTrue(IOUtils.contentEquals(this.getClass().getResourceAsStream("/test-hive-table/hive-test.avsc"),
        new FileInputStream(schemaUrl)));
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    fs.delete(this.testBasePath, true);
  }

}
