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

    // Comma separated
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(ImmutableMap.of("key1", "value1,value2")), "key1"),
        ImmutableList.of("value1", "value2"));

    // Type safe list
    Assert.assertEquals(
        ConfigUtils.getStringList(ConfigFactory.empty().withValue("key1", ConfigValueFactory.fromIterable(ImmutableList.of("value1", "value2"))), "key1"),
        ImmutableList.of("value1", "value2"));

    // Empty list if path does not exist
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(ImmutableMap.of("key1", "value1,value2")), "key2"), ImmutableList.of());

    // Empty list of path is null
    Map<String,String> configMap = Maps.newHashMap();
    configMap.put("key1", null);
    Assert.assertEquals(ConfigUtils.getStringList(ConfigFactory.parseMap(configMap), "key1"), ImmutableList.of());

  }
}
