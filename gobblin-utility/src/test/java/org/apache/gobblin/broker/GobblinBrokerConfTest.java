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

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.ScopedConfigView;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class GobblinBrokerConfTest {

  private static final Joiner JOINER = Joiner.on(".");

  @Test
  public void testCorrectConfigInjection() {

    String key = "myKey";

    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key1"), "value1",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key2"), "value2",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.CONTAINER.name(), "key2"), "value2scope",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, key, "key2"), "value2key",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.CONTAINER.name(), key, "key2"), "value2scopekey"
    ));

    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());

    KeyedScopedConfigViewImpl<GobblinScopeTypes, TestResourceKey> configView =
        topBroker.getConfigView(GobblinScopeTypes.CONTAINER, new TestResourceKey(key), TestFactory.NAME);

    Assert.assertEquals(configView.getScope(), GobblinScopeTypes.CONTAINER);
    Assert.assertEquals(configView.getKey().toConfigurationKey(), key);
    Assert.assertEquals(configView.getKeyedConfig().getString("key2"), "value2key");
    Assert.assertEquals(configView.getScopedConfig().getString("key2"), "value2scope");
    Assert.assertEquals(configView.getKeyedScopedConfig().getString("key2"), "value2scopekey");
    Assert.assertEquals(configView.getFactorySpecificConfig().getString("key1"), "value1");
    Assert.assertEquals(configView.getFactorySpecificConfig().getString("key2"), "value2");
    Assert.assertEquals(configView.getConfig().getString("key2"), "value2scopekey");
    Assert.assertEquals(configView.getConfig().getString("key1"), "value1");

    configView =
        topBroker.getConfigView(GobblinScopeTypes.TASK, new TestResourceKey(key), TestFactory.NAME);
    Assert.assertEquals(configView.getConfig().getString("key2"), "value2key");

  }

  @Test
  public void testOverrides() {
    String key = "myKey";

    // Correct creation behavior
    Config config = ConfigFactory.parseMap(ImmutableMap.<String, String>builder()
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key1"), "value1")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key2"), "value2")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.CONTAINER.name(), "key2"), "value2scope")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, key, "key2"), "value2key")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.CONTAINER.name(), key, "key2"), "value2scopekey")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.JOB.name(), "key2"), "value2scope")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.JOB.name(), key, "key2"), "value2scopekey")
        .build());

    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());

    Config overrides = ConfigFactory.parseMap(ImmutableMap.<String, String>builder()
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key1"), "value1_o")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key2"), "value2_o")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.CONTAINER.name(), "key2"), "value2scope_o")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, key, "key2"), "value2key_o")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.CONTAINER.name(), key, "key2"), "value2scopekey_o")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.JOB.name(), "key2"), "value2scope_o")
        .put(JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopeTypes.JOB.name(), key, "key2"), "value2scopekey_o")
        .build());

    SharedResourcesBrokerImpl<GobblinScopeTypes> jobBroker =
        topBroker.newSubscopedBuilder(new JobScopeInstance("myJob", "job123")).
            withOverridingConfig(overrides).build();

    ScopedConfigView<GobblinScopeTypes, TestResourceKey> configView =
        jobBroker.getConfigView(GobblinScopeTypes.CONTAINER, new TestResourceKey(key), TestFactory.NAME);
    Assert.assertEquals(configView.getConfig().getString("key1"), "value1");
    Assert.assertEquals(configView.getConfig().getString("key2"), "value2scopekey");

    configView =
        jobBroker.getConfigView(GobblinScopeTypes.JOB, new TestResourceKey(key), TestFactory.NAME);
    Assert.assertEquals(configView.getConfig().getString("key1"), "value1");
    Assert.assertEquals(configView.getConfig().getString("key2"), "value2scopekey_o");

  }
}
