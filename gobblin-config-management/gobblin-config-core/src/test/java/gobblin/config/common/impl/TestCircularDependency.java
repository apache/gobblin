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

public class TestCircularDependency {
  
  private final String version = "V1.0";
  List<ConfigKeyPath> emptyList = Collections.emptyList();

  @Test
  public void testSelfImportSelf() {
    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
    
    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);
    List<ConfigKeyPath> rootChildren = new ArrayList<ConfigKeyPath>();
    rootChildren.add(tag);
    when(mockConfigStore.getChildren(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(rootChildren);
    when(mockConfigStore.getChildren(tag, version)).thenReturn(emptyList);
    
    // self import self
    List<ConfigKeyPath> tagImports = new ArrayList<ConfigKeyPath>();
    tagImports.add(tag);
    when(mockConfigStore.getOwnImports(tag, version)).thenReturn(tagImports);
    
    when(mockConfigStore.getOwnImports(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(emptyList);
    
    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);
    
   try {
      inMemory.getImportsRecursively(tag);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("/tag -> /tag") > 0);
    }
  }
  
//  @Test
//  public void testSelfImportChild() {
//    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
//    ConfigKeyPath highPriorityTag = tag.createChild("highPriorityTag");
//    
//    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
//    when(mockConfigStore.getCurrentVersion()).thenReturn(version);
//    List<ConfigKeyPath> rootChildren = new ArrayList<ConfigKeyPath>();
//    rootChildren.add(tag);
//    when(mockConfigStore.getChildren(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(rootChildren);
//    
//    List<ConfigKeyPath> tagChildren = new ArrayList<ConfigKeyPath>();
//    tagChildren.add(highPriorityTag);
//    when(mockConfigStore.getChildren(tag, version)).thenReturn(tagChildren);
//    
//    when(mockConfigStore.getChildren(highPriorityTag, version)).thenReturn(this.emptyList);
//    
//    // parent import direct child
//    List<ConfigKeyPath> tagImports = new ArrayList<ConfigKeyPath>();
//    tagImports.add(highPriorityTag);
//    when(mockConfigStore.getOwnImports(tag, version)).thenReturn(tagImports);
//    
//    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
//    InMemoryTopology inMemory = new InMemoryTopology(csTopology);
//    
//   try {
//      inMemory.getImportsRecursively(tag);
//      Assert.fail("Did not catch expected CircularDependencyException");
//    } catch (CircularDependencyException e) {
//      Assert.assertTrue(e.getMessage().indexOf("/tag -> /tag/highPriorityTag -> /tag") > 0);
//    }
//  }
//  
//  @Test
//  public void testSelfImportDescendant() {
//    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
//    ConfigKeyPath highPriorityTag = tag.createChild("highPriorityTag");
//    ConfigKeyPath nertzHighPriorityTag = highPriorityTag.createChild("nertzHighPriorityTag");
//    
//    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
//    when(mockConfigStore.getCurrentVersion()).thenReturn(version);
//    List<ConfigKeyPath> rootChildren = new ArrayList<ConfigKeyPath>();
//    rootChildren.add(tag);
//    when(mockConfigStore.getChildren(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(rootChildren);
//    
//    List<ConfigKeyPath> tagChildren = new ArrayList<ConfigKeyPath>();
//    tagChildren.add(highPriorityTag);
//    when(mockConfigStore.getChildren(tag, version)).thenReturn(tagChildren);
//    
//    List<ConfigKeyPath> highPriorityTagChildren = new ArrayList<ConfigKeyPath>();
//    highPriorityTagChildren.add(nertzHighPriorityTag);
//    when(mockConfigStore.getChildren(highPriorityTag, version)).thenReturn(highPriorityTagChildren);
//    
//    // self import descendant
//    List<ConfigKeyPath> tagImports = new ArrayList<ConfigKeyPath>();
//    tagImports.add(nertzHighPriorityTag);
//    when(mockConfigStore.getOwnImports(tag, version)).thenReturn(tagImports);
//    
//    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
//    InMemoryTopology inMemory = new InMemoryTopology(csTopology);
//    
//   try {
//      inMemory.getImportsRecursively(tag);
//      Assert.fail("Did not catch expected CircularDependencyException");
//    } catch (CircularDependencyException e) {
//      Assert.assertTrue(e.getMessage().indexOf("/tag -> /tag/highPriorityTag/nertzHighPriorityTag -> /tag/highPriorityTag -> /tag") > 0);
//    }
//  }
//  
//  @Test
//  public void testLoops() {
//    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
//    ConfigKeyPath subTag1 = tag.createChild("subTag1");
//    ConfigKeyPath subTag2 = tag.createChild("subTag2");
//    ConfigKeyPath subTag3 = tag.createChild("subTag3");
//    
//    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
//    when(mockConfigStore.getCurrentVersion()).thenReturn(version);
//    List<ConfigKeyPath> rootChildren = new ArrayList<ConfigKeyPath>();
//    rootChildren.add(tag);
//    when(mockConfigStore.getChildren(SingleLinkedListConfigKeyPath.ROOT, version)).thenReturn(rootChildren);
//    
//    List<ConfigKeyPath> tagChildren = new ArrayList<ConfigKeyPath>();
//    tagChildren.add(subTag1);
//    tagChildren.add(subTag2);
//    tagChildren.add(subTag3);
//    when(mockConfigStore.getChildren(tag, version)).thenReturn(tagChildren);
//    
//    // self import descendant
//    List<ConfigKeyPath> subTag1Imports = new ArrayList<ConfigKeyPath>();
//    subTag1Imports.add(subTag2);
//    when(mockConfigStore.getOwnImports(subTag1, version)).thenReturn(subTag1Imports);
//    
//    List<ConfigKeyPath> subTag2Imports = new ArrayList<ConfigKeyPath>();
//    subTag1Imports.add(subTag3);
//    when(mockConfigStore.getOwnImports(subTag2, version)).thenReturn(subTag2Imports);
//    
//    List<ConfigKeyPath> subTag3Imports = new ArrayList<ConfigKeyPath>();
//    subTag1Imports.add(subTag1);
//    when(mockConfigStore.getOwnImports(subTag3, version)).thenReturn(subTag3Imports);
//    
//    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
//    InMemoryTopology inMemory = new InMemoryTopology(csTopology);
//    
//   try {
//      inMemory.getImportsRecursively(subTag1);
//      Assert.fail("Did not catch expected CircularDependencyException");
//    } catch (CircularDependencyException e) {
//      System.out.println("AAA " + e.getMessage());
//      Assert.assertTrue(e.getMessage().indexOf("/tag -> /tag/highPriorityTag/nertzHighPriorityTag -> /tag/highPriorityTag -> /tag") > 0);
//    }
//  }
}
