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

package org.apache.gobblin.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.jasypt.util.text.BasicTextEncryptor;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

import static org.assertj.core.api.Assertions.assertThat;


public class ConfigUtilsTest {

  @Test
  public void testSaveConfigToFile()
      throws IOException {
    FileUtils fileUtils = new FileUtils();
    ConfigUtils configUtils = new ConfigUtils(fileUtils);
    ImmutableMap<String, String> configMap = ImmutableMap.of("k1", "v1", "k2", "v2");
    Config config = ConfigFactory.parseMap(configMap);
    Path destPath = Paths.get("test-config-file.txt");

    configUtils.saveConfigToFile(config, destPath);
    Config restoredConfig = ConfigFactory.parseFile(destPath.toFile());

    assertThat(restoredConfig.getString("k1")).isEqualTo("v1");
    assertThat(restoredConfig.getString("k2")).isEqualTo("v2");

    java.nio.file.Files.deleteIfExists(destPath);
  }


  @Test
  public void testPropertiesToConfig() {

    Properties properties = new Properties();
    properties.setProperty("k1.kk1", "v1");
    properties.setProperty("k1.kk2", "v2");
    properties.setProperty("k2.kk", "v3");
    properties.setProperty("k3", "v4");
    properties.setProperty("k3.kk1", "v5");
    properties.setProperty("k3.kk1.kkk1", "v6");

    Config conf = ConfigUtils.propertiesToConfig(properties);
    Assert.assertEquals(conf.getString("k1.kk1"), "v1");
    Assert.assertEquals(conf.getString("k1.kk2"), "v2");
    Assert.assertEquals(conf.getString("k2.kk"), "v3");
    Assert.assertEquals(conf.getString(ConfigUtils.sanitizeFullPrefixKey("k3")), "v4");
    Assert.assertEquals(conf.getString(ConfigUtils.sanitizeFullPrefixKey("k3.kk1")), "v5");
    Assert.assertEquals(conf.getString("k3.kk1.kkk1"), "v6");

  }

  @Test
  public void testPropertiesToConfigWithPrefix() {

    Properties properties = new Properties();
    properties.setProperty("k1.kk1", "v1");
    properties.setProperty("k1.kk2", "v2");
    properties.setProperty("k2.kk", "v3");

    Config conf = ConfigUtils.propertiesToConfig(properties, Optional.of("k1"));
    Assert.assertEquals(conf.getString("k1.kk1"), "v1");
    Assert.assertEquals(conf.getString("k1.kk2"), "v2");
    Assert.assertFalse(conf.hasPath("k2.kk"), "Should not contain key k2.kk");

  }

  @Test
  public void testHasNonEmptyPath() throws Exception {
    Assert.assertTrue(ConfigUtils.hasNonEmptyPath(ConfigFactory.parseMap(ImmutableMap.of("key1", "value1")), "key1"));
    Assert.assertFalse(ConfigUtils.hasNonEmptyPath(ConfigFactory.parseMap(ImmutableMap.of("key2", "value1")), "key1"));
    Assert.assertFalse(ConfigUtils.hasNonEmptyPath(ConfigFactory.parseMap(ImmutableMap.of("key1", "")), "key1"));
  }

  @Test
  public void testGetStringList() throws Exception {

    // values as comma separated strings
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(ImmutableMap.of("a.b", "1,2,3")), "a.b"),
        ImmutableList.of("1", "2", "3"));

    // values as quoted comma separated strings
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(ImmutableMap.of("a.b", "\"1\",\"2\",\"3\"")), "a.b"),
        ImmutableList.of("1", "2", "3"));

    // values as quoted comma separated strings (Multiple values)
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(ImmutableMap.of("a.b", "\"1\",\"2,3\"")), "a.b"),
        ImmutableList.of("1", "2,3"));

    // values as Type safe list
    Assert.assertEquals(ConfigUtils.getStringList(
            ConfigFactory.empty().withValue("a.b",
                ConfigValueFactory.fromIterable(ImmutableList.of("1", "2","3"))), "a.b"),
        ImmutableList.of("1", "2", "3"));

    // values as quoted Type safe list
    Assert.assertEquals(ConfigUtils.getStringList(
            ConfigFactory.empty().withValue("a.b",
                ConfigValueFactory.fromIterable(ImmutableList.of("\"1\"", "\"2\"","\"3\""))), "a.b"),
        ImmutableList.of("1", "2", "3"));

    // values as quoted Type safe list (Multiple values)
    Assert.assertEquals(ConfigUtils.getStringList(
            ConfigFactory.empty().withValue("a.b",
                ConfigValueFactory.fromIterable(ImmutableList.of("\"1\"", "\"2,3\""))), "a.b"),
        ImmutableList.of("1", "2,3"));

    // Empty list if path does not exist
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(ImmutableMap.of("key1", "value1,value2")), "key2"), ImmutableList.of());

    // Empty list of path is null
    Map<String,String> configMap = Maps.newHashMap();
    configMap.put("key1", null);
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(configMap), "key1"), ImmutableList.of());

    // Empty list if value is empty string
    configMap = Maps.newHashMap();
    configMap.put("key2", "");
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(configMap), "key2"), ImmutableList.of());
  }

  @Test
  public void testConfigToProperties() {
    Config cfg = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("key1", 1)
        .put("key2", "sTring")
        .put("key3", true)
        .build());

    Properties props = ConfigUtils.configToProperties(cfg);
    Assert.assertEquals(props.getProperty("key1"), "1");
    Assert.assertEquals(props.getProperty("key2"), "sTring");
    Assert.assertEquals(props.getProperty("key3"), "true");
  }

  @Test
  public void testPropertiesToConfigToState() {

    Properties properties = new Properties();
    properties.setProperty("k1.kk1", "v1");
    properties.setProperty("k1.kk2", "v2");
    properties.setProperty("k2.kk", "v3");
    properties.setProperty("k3", "v4");
    properties.setProperty("k3.kk1", "v5");
    properties.setProperty("k3.kk1.kkk1", "v6");

    Config conf = ConfigUtils.propertiesToConfig(properties);
    State state = ConfigUtils.configToState(conf);
    Assert.assertEquals(state.getProp("k1.kk1"), "v1");
    Assert.assertEquals(state.getProp("k1.kk2"), "v2");
    Assert.assertEquals(state.getProp("k2.kk"), "v3");
    Assert.assertEquals(state.getProp("k3"), "v4");
    Assert.assertEquals(state.getProp("k3.kk1"), "v5");
    Assert.assertEquals(state.getProp("k3.kk1.kkk1"), "v6");
  }

  @Test
  public void testConfigToPropertiesWithPrefix() {
    Config cfg = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("a.key1", 1)
        .put("b.key2", "sTring")
        .put("a.key3", true)
        .build());

    Properties props = ConfigUtils.configToProperties(cfg, "a.");
    Assert.assertEquals(props.getProperty("a.key1"), "1");
    Assert.assertNull(props.getProperty("b.key2"));
    Assert.assertEquals(props.getProperty("a.key3"), "true");
  }

  /**
   * Test that you can go from properties to Config and back without changing.
   * Specifically tests prefixed paths and numeric key-parts.
   */
  @Test
  public void testPropertiesToConfigAndBack() {
    Properties props = new Properties();

    props.setProperty("writer.staging.dir", "foobar");
    props.setProperty("writer.staging.dir.0", "foobar-0");

    Config config = ConfigUtils.propertiesToConfig(props);

    Properties configProps = ConfigUtils.configToProperties(config);

    Assert.assertEquals(configProps, props);
  }


  @Test
  public void testFindFullPrefixKeys() {
    Properties props = new Properties();
    props.setProperty("a.b", "123");
    props.setProperty("a.b1", "123");
    props.setProperty("b", "123");
    props.setProperty("b_a", "123");
    props.setProperty("a.b.c", "123");
    props.setProperty("a.b.c.d.e", "123");
    props.setProperty("b.a", "123");

    Set<String> fullPrefixKeys =
        ConfigUtils.findFullPrefixKeys(props, Optional.<String>absent());
    Assert.assertEquals(fullPrefixKeys, new HashSet<>(Arrays.asList("a.b", "a.b.c", "b")));

    fullPrefixKeys =
        ConfigUtils.findFullPrefixKeys(props, Optional.of("a."));
    Assert.assertEquals(fullPrefixKeys, new HashSet<>(Arrays.asList("a.b", "a.b.c")));

    fullPrefixKeys =
        ConfigUtils.findFullPrefixKeys(props, Optional.of("c."));
    Assert.assertTrue(fullPrefixKeys.isEmpty());

    props = new Properties();
    props.setProperty("a.b", "123");
    props.setProperty("a.b1", "123");
    props.setProperty("b", "123");
    props.setProperty("b_a", "123");
    fullPrefixKeys =
        ConfigUtils.findFullPrefixKeys(props, Optional.<String>absent());
    Assert.assertTrue(fullPrefixKeys.isEmpty());
  }

  @Test
  public void testConfigResolveEncrypted() throws IOException {
    Map<String, String> vals = Maps.newHashMap();
    vals.put("test.key1", "test_val1");
    vals.put("test.key2", "test_val2");

    State state = new State();
    for (Map.Entry<String, String> entry : vals.entrySet()) {
      state.setProp(entry.getKey(), entry.getValue());
    }

    String key = UUID.randomUUID().toString();
    File keyFile = newKeyFile(key);
    state.setProp(ConfigurationKeys.ENCRYPT_KEY_LOC, keyFile.getAbsolutePath());

    Map<String, String> encryptedVals = Maps.newHashMap();
    encryptedVals.put("my.nested.key1", "val1");
    encryptedVals.put("my.nested.key2", "val2");

    String encPrefix = "testenc";
    for (Map.Entry<String, String> entry : encryptedVals.entrySet()) {
      BasicTextEncryptor encryptor = new BasicTextEncryptor();
      encryptor.setPassword(key);
      String encrypted = "ENC(" + encryptor.encrypt(entry.getValue()) + ")";
      state.setProp(encPrefix + "." + entry.getKey(), encrypted);
    }

    Config config = ConfigUtils.resolveEncrypted(ConfigUtils.propertiesToConfig(state.getProperties()), Optional.of(encPrefix));
    Map<String, String> expected = ImmutableMap.<String, String>builder()
        .putAll(vals)
        .putAll(encryptedVals)
        .build();
    for (Map.Entry<String, String> entry : expected.entrySet()) {
      String val = config.getString(entry.getKey());
      Assert.assertEquals(val, entry.getValue());
    }
    keyFile.delete();
  }

  private File newKeyFile(String masterPwd) throws IOException {
    File masterPwdFile = File.createTempFile("masterPassword", null);
    masterPwdFile.deleteOnExit();
    Files.write(masterPwd, masterPwdFile, Charset.defaultCharset());
    return masterPwdFile;
  }
}
