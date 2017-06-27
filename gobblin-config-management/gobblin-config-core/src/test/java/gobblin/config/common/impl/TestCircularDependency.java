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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;


@Test(groups = { "gobblin.config.common.impl" })
public class TestCircularDependency {

  private final String version = "V1.0";
  List<ConfigKeyPath> emptyList = Collections.emptyList();

  public static void addConfigStoreChildren(ConfigStore mockup, String version, ConfigKeyPath parent,
      ConfigKeyPath... configKeyPaths) {
    List<ConfigKeyPath> children = new ArrayList<ConfigKeyPath>();
    for (ConfigKeyPath p : configKeyPaths) {
      children.add(p);
    }

    when(mockup.getChildren(parent, version)).thenReturn(children);
  }

  public static void addConfigStoreImports(ConfigStore mockup, String version, ConfigKeyPath self,
      ConfigKeyPath... configKeyPaths) {
    List<ConfigKeyPath> ownImports = new ArrayList<ConfigKeyPath>();
    for (ConfigKeyPath p : configKeyPaths) {
      ownImports.add(p);
    }

    when(mockup.getOwnImports(self, version)).thenReturn(ownImports);
  }

  @Test
  public void testSelfImportSelf() {
    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    addConfigStoreChildren(mockConfigStore, version, SingleLinkedListConfigKeyPath.ROOT, tag);

    // self import self
    addConfigStoreImports(mockConfigStore, version, tag, tag);

    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);

    try {
      inMemory.getImportsRecursively(tag);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("/tag") > 0);
    }
  }

  @Test
  public void testSelfImportChild() {
    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
    ConfigKeyPath highPriorityTag = tag.createChild("highPriorityTag");

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    addConfigStoreChildren(mockConfigStore, version, SingleLinkedListConfigKeyPath.ROOT, tag);
    addConfigStoreChildren(mockConfigStore, version, tag, highPriorityTag);

    // parent import direct child
    addConfigStoreImports(mockConfigStore, version, tag, highPriorityTag);

    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);

    try {
      inMemory.getImportsRecursively(tag);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("/tag/highPriorityTag") > 0 && e.getMessage().indexOf("/tag ") > 0);
    }
  }

  @Test
  public void testSelfImportDescendant() {
    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
    ConfigKeyPath highPriorityTag = tag.createChild("highPriorityTag");
    ConfigKeyPath nertzHighPriorityTag = highPriorityTag.createChild("nertzHighPriorityTag");

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    addConfigStoreChildren(mockConfigStore, version, SingleLinkedListConfigKeyPath.ROOT, tag);
    addConfigStoreChildren(mockConfigStore, version, tag, highPriorityTag);
    addConfigStoreChildren(mockConfigStore, version, highPriorityTag, nertzHighPriorityTag);

    // self import descendant
    // formed the loop /tag -> /tag/highPriorityTag/nertzHighPriorityTag -> /tag/highPriorityTag -> /tag
    addConfigStoreImports(mockConfigStore, version, tag, nertzHighPriorityTag);

    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);

    try {
      inMemory.getImportsRecursively(tag);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("/tag/highPriorityTag/nertzHighPriorityTag") > 0
          && e.getMessage().indexOf("/tag/highPriorityTag ") > 0 && e.getMessage().indexOf("/tag ") > 0);
    }
  }

  @Test
  public void testSelfIndirectlyImportDescendant() {
    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
    ConfigKeyPath highPriorityTag = tag.createChild("highPriorityTag");
    ConfigKeyPath nertzHighPriorityTag = highPriorityTag.createChild("nertzHighPriorityTag");

    ConfigKeyPath tag2 = SingleLinkedListConfigKeyPath.ROOT.createChild("tag2");

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    addConfigStoreChildren(mockConfigStore, version, SingleLinkedListConfigKeyPath.ROOT, tag, tag2);
    addConfigStoreChildren(mockConfigStore, version, tag, highPriorityTag);
    addConfigStoreChildren(mockConfigStore, version, highPriorityTag, nertzHighPriorityTag);

    // self import descendant
    // formed the loop /tag -> /tag2 -> /tag/highPriorityTag/nertzHighPriorityTag -> /tag/highPriorityTag -> /tag
    addConfigStoreImports(mockConfigStore, version, tag, tag2);
    addConfigStoreImports(mockConfigStore, version, tag2, nertzHighPriorityTag);

    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);

    try {
      inMemory.getImportsRecursively(tag);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("/tag/highPriorityTag/nertzHighPriorityTag") > 0
          && e.getMessage().indexOf("/tag/highPriorityTag ") > 0 && e.getMessage().indexOf("/tag ") > 0
          && e.getMessage().indexOf("/tag2 ") > 0);
    }
  }

  @Test
  public void testLoops() {
    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
    ConfigKeyPath subTag1 = tag.createChild("subTag1");
    ConfigKeyPath subTag2 = tag.createChild("subTag2");
    ConfigKeyPath subTag3 = tag.createChild("subTag3");

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    addConfigStoreChildren(mockConfigStore, version, SingleLinkedListConfigKeyPath.ROOT, tag);
    addConfigStoreChildren(mockConfigStore, version, tag, subTag1, subTag2, subTag3);

    // self import descendant
    // formed loop /tag/subTag1 -> /tag/subTag2 -> /tag/subTag3 -> /tag/subTag1
    addConfigStoreImports(mockConfigStore, version, subTag1, subTag2);
    addConfigStoreImports(mockConfigStore, version, subTag2, subTag3);
    addConfigStoreImports(mockConfigStore, version, subTag3, subTag1);

    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);

    try {
      inMemory.getImportsRecursively(subTag1);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("/tag/subTag1") > 0 && e.getMessage().indexOf("/tag/subTag2") > 0
          && e.getMessage().indexOf("/tag/subTag3") > 0);
    }
  }

  @Test
  public void testNoCircular() {
    ConfigKeyPath tag = SingleLinkedListConfigKeyPath.ROOT.createChild("tag");
    ConfigKeyPath highPriorityTag = tag.createChild("highPriorityTag");
    ConfigKeyPath nertzHighPriorityTag = highPriorityTag.createChild("nertzHighPriorityTag");

    ConfigKeyPath tag2 = SingleLinkedListConfigKeyPath.ROOT.createChild("tag2");

    ConfigStore mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    when(mockConfigStore.getCurrentVersion()).thenReturn(version);

    addConfigStoreChildren(mockConfigStore, version, SingleLinkedListConfigKeyPath.ROOT, tag, tag2);
    addConfigStoreChildren(mockConfigStore, version, tag, highPriorityTag);
    addConfigStoreChildren(mockConfigStore, version, highPriorityTag, nertzHighPriorityTag);

    // mock up imports, point to same node but without circular
    addConfigStoreImports(mockConfigStore, version, nertzHighPriorityTag, tag2);
    addConfigStoreImports(mockConfigStore, version, tag2, tag);

    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(mockConfigStore, this.version);
    InMemoryTopology inMemory = new InMemoryTopology(csTopology);

    List<ConfigKeyPath> result = inMemory.getImportsRecursively(nertzHighPriorityTag);
    Assert.assertEquals(result.size(), 4);
    Iterator<ConfigKeyPath> it = result.iterator();
    Assert.assertEquals(it.next(), tag2);
    Assert.assertEquals(it.next(), tag);
    Assert.assertTrue(it.next().isRootPath());
    Assert.assertEquals(it.next(), highPriorityTag);
  }
}
