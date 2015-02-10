/* (c) 2014 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.metastore;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import gobblin.configuration.State;


/**
 * Unit tests for {@link FsStateStore}.
 */
@Test(groups = {"gobblin.metastore"})
public class FsStateStoreTest {

  private StateStore stateStore;

  @BeforeClass
  public void setUp()
      throws IOException {
    this.stateStore = new FsStateStore("file:///", "metastore-test", State.class);
  }

  @Test
  public void testPut()
      throws IOException {
    List<State> states = Lists.newArrayList();

    State state1 = new State();
    state1.setId("s1");
    state1.setProp("k1", "v1");
    states.add(state1);

    State state2 = new State();
    state2.setId("s2");
    state2.setProp("k2", "v2");
    states.add(state2);

    State state3 = new State();
    state3.setId("s3");
    state3.setProp("k3", "v3");
    states.add(state3);

    Assert.assertFalse(this.stateStore.exists("testStore", "testTable"));
    this.stateStore.putAll("testStore", "testTable", states);
    Assert.assertTrue(this.stateStore.exists("testStore", "testTable"));
  }

  @Test(dependsOnMethods = {"testPut"})
  public void testGet()
      throws IOException {
    List<? extends State> states = this.stateStore.getAll("testStore", "testTable");
    Assert.assertEquals(states.size(), 3);

    Assert.assertEquals(states.get(0).getProp("k1"), "v1");
    Assert.assertEquals(states.get(1).getProp("k2"), "v2");
    Assert.assertEquals(states.get(2).getProp("k3"), "v3");
  }

  @Test(dependsOnMethods = {"testPut"})
  public void testCreateAlias()
      throws IOException {
    this.stateStore.createAlias("testStore", "testTable", "testTable1");
    Assert.assertTrue(this.stateStore.exists("testStore", "testTable1"));
  }

  @Test(dependsOnMethods = {"testCreateAlias"})
  public void testGetAlias()
      throws IOException {
    List<? extends State> states = this.stateStore.getAll("testStore", "testTable1");
    Assert.assertEquals(states.size(), 3);

    Assert.assertEquals(states.get(0).getProp("k1"), "v1");
    Assert.assertEquals(states.get(1).getProp("k2"), "v2");
    Assert.assertEquals(states.get(2).getProp("k3"), "v3");
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration(false));
    Path rootDir = new Path("metastore-test");
    if (fs.exists(rootDir)) {
      fs.delete(rootDir, true);
    }
  }
}
