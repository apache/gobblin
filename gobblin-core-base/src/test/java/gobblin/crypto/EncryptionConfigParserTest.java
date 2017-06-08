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
package gobblin.crypto;

import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;


public class EncryptionConfigParserTest {
  private EncryptionConfigParser parser;

  @BeforeTest
  public void initParser() {
    parser = new EncryptionConfigParser();
  }

  @Test
  public void testValidConfigOneBranch() {
    testWithWriterPrefix(1, 0);
  }

  @Test
  public void testValidConfigSeparateBranch() {
    testWithWriterPrefix(3, 1);
  }

  @Test
  public void testAlgorithmNotPresent() {
    Properties properties = new Properties();
    properties
        .put(EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY,
            "/tmp/foobar");
    properties.put(
        EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY,
        "abracadabra");

    State s = new State(properties);

    Map<String, Object> parsedProperties = EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.WRITER, s, 1, 0);
    Assert.assertNull(parsedProperties, "Expected encryption be empty if no algorithm specified");
  }

  @Test
  public void testProperPrefix() {
    Properties properties = new Properties();
    properties.put(EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY,
        "any");
    properties
        .put(EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY,
            "/tmp/foobar");
    properties.put(
        EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY,
        "abracadabra");
    properties.put(EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "abc.def", "foobar");

    State s = new State(properties);

    Map<String, Object> parsedProperties = EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.WRITER, s, 1, 0);
    Assert.assertNotNull(parsedProperties, "Expected parser to only return one record");
    Assert.assertEquals(parsedProperties.size(), 3, "Did not expect abc.def to be picked up in config");
  }

  @Test
  public void testConverter() {
    WorkUnitState wuState = new WorkUnitState();
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "any");
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY,
        "/tmp/foobar");
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY,
        "abracadabra");
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEY_NAME,
        "keyname");
    wuState.setProp(EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "abc.def", "foobar");

    Map<String, Object> parsedProperties = EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER, wuState);
    Assert.assertNotNull(parsedProperties, "Expected parser to only return one record");
    Assert.assertEquals(parsedProperties.size(), 4, "Did not expect abc.def to be picked up in config");

    Map<String, Object> parsedWriterProperties = EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.WRITER, wuState);
    Assert.assertNull(parsedWriterProperties, "Did not expect to find writer properties");
  }

  @Test
  public void testConverterWithEntityPrefix() {
    final String entityName = "MyConverter";

    WorkUnitState wuState = new WorkUnitState();
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "any");
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + entityName + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "aes_rotating");
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY,
        "/tmp/foobar");
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY,
        "abracadabra");
    wuState.getJobState().setProp(
        EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEY_NAME,
        "keyname");
    wuState.setProp(EncryptionConfigParser.CONVERTER_ENCRYPT_PREFIX + "abc.def", "foobar");

    Map<String, Object> parsedProperties = EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.CONVERTER, entityName, wuState);
    Assert.assertNotNull(parsedProperties, "Expected parser to only return one record");
    Assert.assertEquals(parsedProperties.size(), 4, "Did not expect abc.def to be picked up in config");
    Assert.assertEquals(EncryptionConfigParser.getEncryptionType(parsedProperties), "aes_rotating");
    Map<String, Object> parsedWriterProperties = EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.WRITER, wuState);
    Assert.assertNull(parsedWriterProperties, "Did not expect to find writer properties");
  }

  private void testWithWriterPrefix(int numBranches, int branch) {
    String branchString = "";
    if (numBranches > 1) {
      branchString = String.format(".%d", branch);
    }

    Properties properties = new Properties();
    properties.put(EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY
        + branchString, "any");
    properties.put(
        EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY
            + branchString, "/tmp/foobar");
    properties.put(
        EncryptionConfigParser.WRITER_ENCRYPT_PREFIX + "." + EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY
            + branchString, "abracadabra");

    State s = new State(properties);

    Map<String, Object> parsedProperties = EncryptionConfigParser.getConfigForBranch(EncryptionConfigParser.EntityType.WRITER, s, numBranches, branch);
    Assert.assertNotNull(parsedProperties, "Expected parser to only return one record");

    Assert.assertEquals(EncryptionConfigParser.getEncryptionType(parsedProperties), "any");
    Assert.assertEquals(EncryptionConfigParser.getKeystorePath(parsedProperties), "/tmp/foobar");
    Assert.assertEquals(EncryptionConfigParser.getKeystorePassword(parsedProperties), "abracadabra");
  }
}
