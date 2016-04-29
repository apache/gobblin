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

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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
}
