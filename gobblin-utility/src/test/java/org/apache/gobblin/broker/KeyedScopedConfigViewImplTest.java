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

package org.apache.gobblin.broker;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;


public class KeyedScopedConfigViewImplTest {

  private static final Joiner JOINER = Joiner.on(".");

  @Test
  public void test() {

    String key = "myKey";

    // Correct creation behavior
    Config config = ConfigFactory.parseMap(ImmutableMap.<String, String>builder()
        .put("key1", "value1")
        .put("key2", "value2")
        .put(JOINER.join(key, "key2"), "value2key")
        .put(JOINER.join(GobblinScopeTypes.JOB.name(), "key2"), "value2scope")
        .put(JOINER.join(GobblinScopeTypes.JOB.name(), key, "key2"), "value2scopekey")
        .build());

    KeyedScopedConfigViewImpl<GobblinScopeTypes, TestResourceKey> configView =
        new KeyedScopedConfigViewImpl<>(GobblinScopeTypes.JOB, new TestResourceKey(key), TestFactory.NAME, config);

    Assert.assertEquals(configView.getScope(), GobblinScopeTypes.JOB);
    Assert.assertEquals(configView.getKey().toConfigurationKey(), key);
    Assert.assertEquals(configView.getKeyedConfig().getString("key2"), "value2key");
    Assert.assertEquals(configView.getScopedConfig().getString("key2"), "value2scope");
    Assert.assertEquals(configView.getKeyedScopedConfig().getString("key2"), "value2scopekey");
    Assert.assertEquals(configView.getFactorySpecificConfig().getString("key1"), "value1");
    Assert.assertEquals(configView.getFactorySpecificConfig().getString("key2"), "value2");
    Assert.assertEquals(configView.getConfig().getString("key2"), "value2scopekey");
    Assert.assertEquals(configView.getConfig().getString("key1"), "value1");

  }

}
