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
    /**
     * each node will have a common key "generalKey" with value as "generalValue_${node}"
     * this key will be overwrite
     *
     * each node will have own key "keyOf_${node}" with value "valueOf_${node}"
     * this key will be inherent
     */
    // mock up the configuration values for root
    Map<String, String> rootMap = new HashMap<>();
    rootMap.put("keyOf_Root", "valueOf_Root");
    rootMap.put("generalKey", "generalValue_root"); // keys will be overwrite
    when(mockConfigStore.getOwnConfig(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(ConfigFactory.parseMap(rootMap));

    // mock up the configuration values for /data
    Map<String, String> dataMap = new HashMap<>();
    dataMap.put("keyOf_data", "valueOf_data");
    dataMap.put("generalKey", "generalValue_data");
    when(mockConfigStore.getOwnConfig(data, version)).thenReturn(ConfigFactory.parseMap(dataMap));

    // mock up the configuration values for /data/databases
    Map<String, String> databasesMap = new HashMap<>();
    databasesMap.put("keyOf_databases", "valueOf_databases");
    databasesMap.put("generalKey", "generalValue_data_databases");
    when(mockConfigStore.getOwnConfig(databases, version)).thenReturn(ConfigFactory.parseMap(databasesMap));

    // mock up the configuration values for /data/databases/identity
    Map<String, String> identityMap = new HashMap<>();
    identityMap.put("keyOf_identity", "valueOf_identity");
    identityMap.put("generalKey", "generalValue_data_databases_identity");
    when(mockConfigStore.getOwnConfig(identity, version)).thenReturn(ConfigFactory.parseMap(identityMap));

    // mock up the configuration values for /tag
    Map<String, String> tagMap = new HashMap<>();
    tagMap.put("keyOf_tag", "valueOf_tag");
    tagMap.put("generalKey", "generalValue_tag");
    when(mockConfigStore.getOwnConfig(tag, version)).thenReturn(ConfigFactory.parseMap(tagMap));

    // mock up the configuration values for /tag/espressoTag
    Map<String, String> espressoTagMap = new HashMap<>();
    espressoTagMap.put("keyOf_espressoTag", "valueOf_espressoTag");
    espressoTagMap.put("generalKey", "generalValue_tag_espressoTag");
    when(mockConfigStore.getOwnConfig(espressoTag, version)).thenReturn(ConfigFactory.parseMap(espressoTagMap));

    // mock up the configuration values for /tag/highPriorityTag
    Map<String, String> highPriorityTagMap = new HashMap<>();
    highPriorityTagMap.put("keyOf_highPriorityTag", "valueOf_highPriorityTag");
    highPriorityTagMap.put("generalKey", "generalValue_tag_highPriorityTag");
    when(mockConfigStore.getOwnConfig(highPriorityTag, version)).thenReturn(ConfigFactory.parseMap(highPriorityTagMap));

    // mock up the configuration values for /tag2
    Map<String, String> tag2Map = new HashMap<>();
    tag2Map.put("keyOf_tag2", "valueOf_tag2");
    tag2Map.put("generalKey", "generalValue_tag2");
    when(mockConfigStore.getOwnConfig(tag2, version)).thenReturn(ConfigFactory.parseMap(tag2Map));

    // mock up the configuration values for /tag2/nertzTag2
    Map<String, String> nertzTag2Map = new HashMap<>();
    nertzTag2Map.put("keyOf_nertzTag2", "valueOf_nertzTag2");
    nertzTag2Map.put("generalKey", "generalValue_tag2_nertzTag2");
    when(mockConfigStore.getOwnConfig(nertzTag2, version)).thenReturn(ConfigFactory.parseMap(nertzTag2Map));
  }

  private void checkValuesForIdentity(Config resolvedConfig){
    Assert.assertTrue(resolvedConfig.getString("keyOf_data").equals("valueOf_data"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_identity").equals("valueOf_identity"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_espressoTag").equals("valueOf_espressoTag"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_Root").equals("valueOf_Root"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_nertzTag2").equals("valueOf_nertzTag2"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_highPriorityTag").equals("valueOf_highPriorityTag"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_tag2").equals("valueOf_tag2"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_tag").equals("valueOf_tag"));
    Assert.assertTrue(resolvedConfig.getString("keyOf_databases").equals("valueOf_databases"));

    Assert.assertTrue(resolvedConfig.getString("generalKey").equals("generalValue_data_databases_identity"));
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

    // importedBy using relative URI
    String[] expectedImportedBy = {"etl-hdfs:/tag/espressoTag", "etl-hdfs:/data/databases/identity"};
    URI nertzTagURI = new URI("etl-hdfs:///tag2/nertzTag2");
    Collection<URI> importedBy = client.getImportedBy(nertzTagURI, false);
    Assert.assertEquals(importedBy.size(), 1);
    Assert.assertEquals(importedBy.iterator().next().toString(), expectedImportedBy[0]);

    importedBy = client.getImportedBy(nertzTagURI, true);
    Assert.assertEquals(importedBy.size(), 2);
    for(URI u: importedBy){
      Assert.assertTrue(u.toString().equals(expectedImportedBy[0]) ||
          u.toString().equals(expectedImportedBy[1]));
    }

    // importedBy using abs URI
    String[] expectedImportedBy_abs = {"etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/tag/espressoTag",
        "etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/data/databases/identity"};
    nertzTagURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/tag2/nertzTag2");
    importedBy = client.getImportedBy(nertzTagURI, false);
    Assert.assertEquals(importedBy.size(), 1);
    Assert.assertEquals(importedBy.iterator().next().toString(), expectedImportedBy_abs[0]);

    importedBy = client.getImportedBy(nertzTagURI, true);
    Assert.assertEquals(importedBy.size(), 2);
    for(URI u: importedBy){
      Assert.assertTrue(u.toString().equals(expectedImportedBy_abs[0]) ||
          u.toString().equals(expectedImportedBy_abs[1]));
    }
  }
}
