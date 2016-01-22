/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.config.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.common.impl.ConfigStoreValueInspector;
import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreFactory;

@Test(groups = { "gobblin.config.common.impl" })

public class TestConfigClient {
  private ConfigStore mockConfigStore;
  private final String version = "V1.0";
  private final ConfigKeyPath data = SingleLinkedListConfigKeyPath.ROOT.createChild("data");
  private final ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
  private final ConfigKeyPath tag2 = SingleLinkedListConfigKeyPath.ROOT.createChild("tag2");

  private final ConfigKeyPath databases = data.createChild("databases");
  private final ConfigKeyPath identity = databases.createChild("identity");

  private final ConfigKeyPath highPriorityTag = tag.createChild("highPriorityTag");
  private final ConfigKeyPath espressoTag = tag.createChild("espressoTag");

  private final ConfigKeyPath nertzTag2 = tag2.createChild("nertzTag2");

  public void printConfig(Config config){
    Set<Map.Entry<String,ConfigValue>> entrySet = config.entrySet();
    for(Map.Entry<String,ConfigValue> entry: entrySet){
      System.out.println("key: " + entry.getKey() + ", value: " + entry.getValue());
    }
  }

  @BeforeClass
  public void setup(){
    // Topology for mock up config store

    //    ├── data
    //    │   └── databases
    //    │       └── identity
    //    ├── tag
    //    │   ├── espressoTag
    //    │   └── highPriorityTag
    //    └── tag2
    //        └── nertzTag2


    mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);

    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    List<ConfigKeyPath> emptyList = Collections.emptyList();
    // mock up parent/children topology
    List<ConfigKeyPath> rootChildren = new ArrayList<ConfigKeyPath>();
    rootChildren.add(data);
    rootChildren.add(tag);
    rootChildren.add(tag2);
    when(mockConfigStore.getChildren(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(rootChildren);

    List<ConfigKeyPath> dataChildren = new ArrayList<ConfigKeyPath>();
    dataChildren.add(databases);
    when(mockConfigStore.getChildren(data, version)).thenReturn(dataChildren);

    List<ConfigKeyPath> databasesChildren = new ArrayList<ConfigKeyPath>();
    databasesChildren.add(identity);
    when(mockConfigStore.getChildren(databases, version)).thenReturn(databasesChildren);

    when(mockConfigStore.getChildren(identity, version)).thenReturn(emptyList);

    List<ConfigKeyPath> tagChildren = new ArrayList<ConfigKeyPath>();
    tagChildren.add(highPriorityTag);
    tagChildren.add(espressoTag);
    when(mockConfigStore.getChildren(tag, version)).thenReturn(tagChildren);

    when(mockConfigStore.getChildren(highPriorityTag, version)).thenReturn(emptyList);
    when(mockConfigStore.getChildren(espressoTag, version)).thenReturn(emptyList);

    List<ConfigKeyPath> tag2Children = new ArrayList<ConfigKeyPath>();
    tag2Children.add(nertzTag2);
    when(mockConfigStore.getChildren(tag2, version)).thenReturn(tag2Children);

    when(mockConfigStore.getChildren(nertzTag2, version)).thenReturn(emptyList);

    // mock up import links
    // identity import espressoTag and highPriorityTag
    List<ConfigKeyPath> identityImports = new ArrayList<ConfigKeyPath>();
    identityImports.add(espressoTag);
    identityImports.add(highPriorityTag);
    when(mockConfigStore.getOwnImports(identity, version)).thenReturn(identityImports);

    // espressoTag imports nertzTag2
    List<ConfigKeyPath> espressoImports = new ArrayList<ConfigKeyPath>();
    espressoImports.add(nertzTag2);
    when(mockConfigStore.getOwnImports(espressoTag, version)).thenReturn(espressoImports);

    mockupConfigValues();
  }

  private void mockupConfigValues(){
    // mock up the configuration values for root
    Map<String, String> rootMap = new HashMap<>();
    rootMap.put("keyInRoot", "valueInRoot");
    when(mockConfigStore.getOwnConfig(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(ConfigFactory.parseMap(rootMap));

    Collection<ConfigKeyPath> currentLevel = mockConfigStore.getChildren(SingleLinkedListConfigKeyPath.ROOT, version);
    while(!currentLevel.isEmpty()){
      Collection<ConfigKeyPath> nextLevel = new ArrayList<ConfigKeyPath>();
      for(ConfigKeyPath p: currentLevel){
        mockupConfigValueForKey(p);
        nextLevel.addAll(mockConfigStore.getChildren(p, version));
      }

      currentLevel = nextLevel;
    }
  }

  private void mockupConfigValueForKey(ConfigKeyPath configKey){
    final String generalKey = "generalKey";
    Map<String, String> valueMap = new HashMap<>();
    // key in all the nodes
    valueMap.put(generalKey, "valueOf_" +generalKey +"_"+configKey.getOwnPathName() );

    // key in self node
    valueMap.put("keyOf_" + configKey.getOwnPathName(), "valueOf_" + configKey.getOwnPathName());
    when(mockConfigStore.getOwnConfig(configKey, version)).thenReturn(ConfigFactory.parseMap(valueMap));
  }

  private void testValues(ConfigStoreValueInspector valueInspector){
    Config ownConfig = valueInspector.getOwnConfig(identity);
    Assert.assertTrue(ownConfig.entrySet().size() == 2 );
    Assert.assertTrue(ownConfig.getString("keyOf_identity").equals("valueOf_identity"));
    Assert.assertTrue(ownConfig.getString("generalKey").equals("valueOf_generalKey_identity"));

    Config resolvedConfig = valueInspector.getResolvedConfig(identity);
    checkValuesForIdentity(resolvedConfig);
  }
  
  private void checkValuesForIdentity(Config resolvedConfig){
    Assert.assertTrue(resolvedConfig.entrySet().size() == 10 );
    Assert.assertTrue(resolvedConfig.getString("keyOf_data").equals("valueOf_data"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_identity").equals("valueOf_identity"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_espressoTag").equals("valueOf_espressoTag"));
    Assert.assertTrue(resolvedConfig.getString("generalKey").equals("valueOf_generalKey_identity"));
    Assert.assertTrue(resolvedConfig.getString("keyInRoot").equals("valueInRoot"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_nertzTag2").equals("valueOf_nertzTag2"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_highPriorityTag").equals("valueOf_highPriorityTag"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_tag2").equals("valueOf_tag2"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_tag").equals("valueOf_tag"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_databases").equals("valueOf_databases"));
  }

  @Test
  private void testFromClient() throws Exception{
    ConfigStoreFactoryRegister mockConfigStoreFactoryRegister;
    ConfigStoreFactory mockConfigStoreFactory;

    URI relativeURI = new URI("etl-hdfs:///data/databases/identity");
    URI absoluteURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/data/databases/identity");
    when(mockConfigStore.getStoreURI()).thenReturn(new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest"));

    mockConfigStoreFactory = mock(ConfigStoreFactory.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStoreFactory.getScheme()).thenReturn("etl-hdfs");
    when(mockConfigStoreFactory.createConfigStore(absoluteURI)).thenReturn(mockConfigStore);
    when(mockConfigStoreFactory.createConfigStore(relativeURI)).thenReturn(mockConfigStore);

    mockConfigStoreFactoryRegister = mock(ConfigStoreFactoryRegister.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStoreFactoryRegister.getConfigStoreFactory("etl-hdfs")).thenReturn(mockConfigStoreFactory);

    ConfigClient client = new ConfigClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY, mockConfigStoreFactoryRegister);
    Config resolved = client.getConfig(relativeURI);
    checkValuesForIdentity(resolved);

    resolved = client.getConfig(absoluteURI);
    checkValuesForIdentity(resolved);
  }
}
