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

package gobblin.config.common.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import gobblin.config.TestEnvironment;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;


public class TestConfigStoreValueInspector {

  private final String version = "1.0";

  /**Set by {@link TestEnvironment#setup()}**/
  public static final String VALUE_INSPECTOR_SYS_PROP_KEY = "sysProp.key1";
  public static final String VALUE_INSPECTOR_SYS_PROP_VALUE = "sysProp.value1";

  @Test
  public void testSystemPropertyResolution() {

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    ConfigStoreTopologyInspector mockTopology = mock(ConfigStoreTopologyInspector.class, Mockito.RETURNS_SMART_NULLS);

    ConfigStoreBackedValueInspector valueInspector =
        new ConfigStoreBackedValueInspector(mockConfigStore, version, mockTopology);

    ConfigKeyPath testConfigKeyPath = SingleLinkedListConfigKeyPath.ROOT.createChild("a");
    when(mockConfigStore.getOwnConfig(testConfigKeyPath.getParent(), version)).thenReturn(ConfigFactory.empty());
    when(mockConfigStore.getOwnConfig(testConfigKeyPath, version)).thenReturn(
        ConfigFactory.parseString("configProp = ${?" + VALUE_INSPECTOR_SYS_PROP_KEY + "}"));

    Assert.assertEquals(valueInspector.getResolvedConfig(testConfigKeyPath).getString("configProp"), VALUE_INSPECTOR_SYS_PROP_VALUE);

  }

  @Test
  public void testResolveConfigOverridingInChild() {

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    ConfigStoreTopologyInspector mockTopology = mock(ConfigStoreTopologyInspector.class, Mockito.RETURNS_SMART_NULLS);

    ConfigStoreBackedValueInspector valueInspector =
        new ConfigStoreBackedValueInspector(mockConfigStore, version, mockTopology);

    ConfigKeyPath keyPathA = SingleLinkedListConfigKeyPath.ROOT.createChild("a");
    ConfigKeyPath keyPathA_Slash_B = keyPathA.createChild("b");

    when(mockConfigStore.getOwnConfig(keyPathA.getParent(), version)).thenReturn(ConfigFactory.empty());
    when(mockConfigStore.getOwnConfig(keyPathA, version)).thenReturn(
        ConfigFactory.parseString("key1 = value1InA \n key2 = ${key1}"));

    when(mockConfigStore.getOwnConfig(keyPathA_Slash_B, version)).thenReturn(
        ConfigFactory.parseString("key1 = value1InB"));

    Assert.assertEquals(valueInspector.getResolvedConfig(keyPathA_Slash_B).getString("key2"), "value1InB");

  }
}
