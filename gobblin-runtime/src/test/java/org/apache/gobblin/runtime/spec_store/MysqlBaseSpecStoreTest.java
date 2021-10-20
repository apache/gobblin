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

package org.apache.gobblin.runtime.spec_store;

import com.google.common.collect.Iterators;
import com.typesafe.config.Config;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.api.FlowSpecSearchObject;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.spec_executorInstance.MockedSpecExecutor;
import org.apache.gobblin.runtime.spec_serde.JavaSpecSerDe;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MysqlBaseSpecStoreTest {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "base_spec_store";

  private MysqlBaseSpecStore specStore;
  private final URI uri1 = new URI(new TopologySpec.Builder().getDefaultTopologyCatalogURI().toString() + "1");
  private final URI uri2 = new URI(new TopologySpec.Builder().getDefaultTopologyCatalogURI().toString() + "2");
  private TopologySpec topoSpec1, topoSpec2;

  public MysqlBaseSpecStoreTest()
      throws URISyntaxException { // (based on `uri1` and other initializations just above)
  }

  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    // prefix keys to demonstrate disambiguation mechanism used to side-step intentially-sabatoged non-prefixed, 'fallback'
    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, " SABATOGE! !" + testDb.getJdbcUrl())
        .addPrimitive(MysqlBaseSpecStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(MysqlBaseSpecStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(MysqlBaseSpecStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(MysqlBaseSpecStore.CONFIG_PREFIX + "." + ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.specStore = new MysqlBaseSpecStore(config, new JavaSpecSerDe());

    topoSpec1 = new TopologySpec.Builder(this.uri1)
        .withConfig(ConfigBuilder.create()
            .addPrimitive("key", "value")
            .addPrimitive("key3", "value3")
            .addPrimitive("config.with.dot", "value4").build())
        .withDescription("Test1")
        .withVersion("Test version")
        .withSpecExecutor(MockedSpecExecutor.createDummySpecExecutor(new URI("execA")))
        .build();
    topoSpec2 = new TopologySpec.Builder(this.uri2)
        .withConfig(ConfigBuilder.create().addPrimitive("converter", "value1,value2,value3")
            .addPrimitive("key3", "value3").build())
        .withDescription("Test2")
        .withVersion("Test version 2")
        .withSpecExecutor(MockedSpecExecutor.createDummySpecExecutor(new URI("execB")))
        .build();
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testSpecSearchUnsupported() throws Exception {
    FlowSpecSearchObject flowSpecSearchObject = FlowSpecSearchObject.builder().build();
    Collection<Spec> specs = this.specStore.getSpecs(flowSpecSearchObject);
  }

  @Test
  public void testAddSpec() throws Exception {
    this.specStore.addSpec(this.topoSpec1);
    this.specStore.addSpec(this.topoSpec2);

    Assert.assertEquals(this.specStore.getSize(), 2);
    Assert.assertTrue(this.specStore.exists(this.uri1));
    Assert.assertTrue(this.specStore.exists(this.uri2));
    Assert.assertFalse(this.specStore.exists(URI.create("dummy")));
  }

  @Test (dependsOnMethods = "testAddSpec")
  public void testGetSpec() throws Exception {
    TopologySpec result = (TopologySpec) this.specStore.getSpec(this.uri1);
    Assert.assertEquals(result, this.topoSpec1);

    Collection<Spec> specs = this.specStore.getSpecs();
    Assert.assertEquals(specs.size(), 2);
    Assert.assertTrue(specs.contains(this.topoSpec1));
    Assert.assertTrue(specs.contains(this.topoSpec1));

    Iterator<URI> uris = this.specStore.getSpecURIs();
    Assert.assertTrue(Iterators.contains(uris, this.uri1));
    Assert.assertTrue(Iterators.contains(uris, this.uri2));
  }

  @Test (dependsOnMethods = "testGetSpec")
  public void testGetSpecWithTag() throws Exception {
    //Creating and inserting specs with tags
    URI uri5 = URI.create("topospec5");
    TopologySpec topoSpec5 = new TopologySpec.Builder(uri5)
        .withConfig(ConfigBuilder.create()
            .addPrimitive("key5", "value5").build())
        .withDescription("Test5")
        .withVersion("Test version 5")
        .withSpecExecutor(MockedSpecExecutor.createDummySpecExecutor(new URI("execE")))
        .build();

    URI uri6 = URI.create("topospec6");
    TopologySpec topoSpec6 = new TopologySpec.Builder(uri6)
        .withConfig(ConfigBuilder.create()
            .addPrimitive("key6", "value6").build())
        .withDescription("Test6")
        .withVersion("Test version 6")
        .withSpecExecutor(MockedSpecExecutor.createDummySpecExecutor(new URI("execF")))
        .build();

    this.specStore.addSpec(topoSpec5, "dr");
    this.specStore.addSpec(topoSpec6, "dr");

    Assert.assertTrue(this.specStore.exists(uri5));
    Assert.assertTrue(this.specStore.exists(uri6));
    List<URI> result = new ArrayList<>();
    this.specStore.getSpecURIsWithTag("dr").forEachRemaining(result::add);
    Assert.assertEquals(result.size(), 2);
  }

  @Test (dependsOnMethods = "testGetSpecWithTag")
  public void testDeleteSpec() throws Exception {
    Assert.assertEquals(this.specStore.getSize(), 4);
    this.specStore.deleteSpec(this.uri1);
    Assert.assertEquals(this.specStore.getSize(), 3);
    Assert.assertFalse(this.specStore.exists(this.uri1));
  }
}