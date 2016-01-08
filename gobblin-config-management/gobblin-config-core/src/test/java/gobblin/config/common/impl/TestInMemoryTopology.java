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

package gobblin.config.common.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;

@Test(groups = { "gobblin.config.common.impl" })

public class TestInMemoryTopology {
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
  }

  @Test
  public void testNonRoot() {
    Assert.assertEquals(mockConfigStore.getCurrentVersion(), version);
    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(this.mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);

    Collection<ConfigKeyPath> result = inMemory.getChildren(data);
    Assert.assertTrue(result.size()==1);
    Assert.assertEquals(result.iterator().next(), databases);

    // test own imports
    result = inMemory.getOwnImports(identity);
    Assert.assertTrue(result.size()==2);
    Iterator<ConfigKeyPath> it = result.iterator();
    Assert.assertEquals(it.next(), espressoTag);
    Assert.assertEquals(it.next(), highPriorityTag);

    // test import recursively
    result = inMemory.getImportsRecursively(identity);
    Assert.assertTrue(result.size()==3);
    it = result.iterator();
    Assert.assertEquals(it.next(), espressoTag);
    Assert.assertEquals(it.next(), nertzTag2);
    Assert.assertEquals(it.next(), highPriorityTag);

    // test own imported by
    result = inMemory.getImportedBy(nertzTag2);
    Assert.assertTrue(result.size()==1);
    Assert.assertEquals(result.iterator().next(), espressoTag);
    
    // test imported by recursively, as the imported by recursively do not care about
    // order, need to use HashSet to test
    result = inMemory.getImportedByRecursively(nertzTag2);
    Set<ConfigKeyPath> expected = new HashSet<ConfigKeyPath>();
    expected.add(espressoTag);
    expected.add(identity);
    Assert.assertTrue(result.size()==2);
    it = result.iterator();
    
    while(it.hasNext()){
      ConfigKeyPath tmp = it.next();
      Assert.assertTrue(expected.contains(tmp));
      expected.remove(tmp);
    }
    //Assert.assertEquals(result.iterator().next(), espressoTag);
    for(ConfigKeyPath p: result){
      System.out.println("imported by recur is " + p);
    }
  }
}
