/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util;

import java.util.Map;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


public class ConfigUtilsTest {

  @Test
  public void testPropertiesToConfig() {

    Properties properties = new Properties();
    properties.setProperty("k1.kk1", "v1");
    properties.setProperty("k1.kk2", "v2");
    properties.setProperty("k2.kk", "v3");

    Config conf = ConfigUtils.propertiesToConfig(properties);
    Assert.assertEquals(conf.getString("k1.kk1"), "v1");
    Assert.assertEquals(conf.getString("k1.kk2"), "v2");
    Assert.assertEquals(conf.getString("k2.kk"), "v3");

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
}
