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

package org.apache.gobblin.metastore;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ClassAliasResolver;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.State;


/**
 * Unit tests for {@link FsStateStore}.
 */
@Test(groups = { "gobblin.metastore" })
public class FsStateStoreTest {
  private StateStore<State> stateStore;
  private StateStore.Factory stateStoreFactory;
  private Config config;

  @BeforeClass
  public void setUp() throws Exception {
    ClassAliasResolver<StateStore.Factory> resolver =
        new ClassAliasResolver<>(StateStore.Factory.class);

    stateStoreFactory =
        resolver.resolveClass("fs").newInstance();

    config = ConfigFactory.empty().withValue(ConfigurationKeys.STATE_STORE_FS_URI_KEY,
        ConfigValueFactory.fromAnyRef("file:///")).withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
        ConfigValueFactory.fromAnyRef("metastore-test")).withValue("fs.permissions.umask-mode",
        ConfigValueFactory.fromAnyRef("022"));

    this.stateStore = stateStoreFactory.createStateStore(config, State.class);

    // cleanup in case files left behind by a prior run
    this.stateStore.delete("testStore");
  }

  @Test
  public void testPut() throws IOException {
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

  @Test(dependsOnMethods = { "testPut" })
  public void testGet() throws IOException {
    List<State> states = this.stateStore.getAll("testStore", "testTable");
    Assert.assertEquals(states.size(), 3);

    Assert.assertEquals(states.get(0).getProp("k1"), "v1");
    Assert.assertEquals(states.get(1).getProp("k2"), "v2");
    Assert.assertEquals(states.get(2).getProp("k3"), "v3");
  }

  @Test(dependsOnMethods = { "testPut" })
  public void testCreateAlias() throws IOException {
    this.stateStore.createAlias("testStore", "testTable", "testTable1");
    Assert.assertTrue(this.stateStore.exists("testStore", "testTable1"));
  }

  @Test(dependsOnMethods = { "testCreateAlias" })
  public void testGetAlias() throws IOException {
    List<State> states = this.stateStore.getAll("testStore", "testTable1");
    Assert.assertEquals(states.size(), 3);

    Assert.assertEquals(states.get(0).getProp("k1"), "v1");
    Assert.assertEquals(states.get(1).getProp("k2"), "v2");
    Assert.assertEquals(states.get(2).getProp("k3"), "v3");
  }

//  Disable backwards compatibility change, since we are doing a major version upgrade
//  .. and this is related to previous migration.
  @Test
  public void testBackwardsCompat() throws IOException {
    // Tests with a state store that was saved before the WritableShim changes
    Config bwConfig = ConfigFactory.load(config);
    URL path = getClass().getResource("/backwardsCompatTestStore");
    Assert.assertNotNull(path);

    bwConfig = bwConfig.withValue(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY,
        ConfigValueFactory.fromAnyRef(path.toString()));

    StateStore<State> bwStateStore = stateStoreFactory.createStateStore(bwConfig, State.class);
    Assert.assertTrue(bwStateStore.exists("testStore", "testTable"));

    List<State> states = bwStateStore.getAll("testStore", "testTable");
    Assert.assertEquals(states.size(), 3);

    Assert.assertEquals(states.get(0).getProp("k1"), "v1");
    Assert.assertEquals(states.get(1).getProp("k2"), "v2");
    Assert.assertEquals(states.get(2).getProp("k3"), "v3");
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration(false));
    Path rootDir = new Path("metastore-test");
    if (fs.exists(rootDir)) {
      fs.delete(rootDir, true);
    }
  }
}
