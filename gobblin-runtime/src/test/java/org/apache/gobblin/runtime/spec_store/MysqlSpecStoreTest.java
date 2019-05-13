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

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang3.SerializationUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Iterators;
import com.typesafe.config.Config;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metastore.testing.ITestMetastoreDatabase;
import org.apache.gobblin.metastore.testing.TestMetastoreDatabaseFactory;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSerDe;


public class MysqlSpecStoreTest {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "spec_store";

  private MysqlSpecStore specStore;
  private URI uri1 = URI.create("flowspec1");
  private URI uri2 = URI.create("flowspec2");
  private FlowSpec flowSpec1 = FlowSpec.builder(this.uri1)
      .withConfig(ConfigBuilder.create().addPrimitive("key", "value").build())
      .withDescription("Test flow spec")
      .withVersion("Test version")
      .build();
  private FlowSpec flowSpec2 = FlowSpec.builder(this.uri2)
      .withConfig(ConfigBuilder.create().addPrimitive("key2", "value2").build())
      .withDescription("Test flow spec 2")
      .withVersion("Test version 2")
      .build();

  @BeforeClass
  public void setUp() throws Exception {
    ITestMetastoreDatabase testDb = TestMetastoreDatabaseFactory.get();

    Config config = ConfigBuilder.create()
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_URL_KEY, testDb.getJdbcUrl())
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_USER_KEY, USER)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY, PASSWORD)
        .addPrimitive(ConfigurationKeys.STATE_STORE_DB_TABLE_KEY, TABLE)
        .build();

    this.specStore = new MysqlSpecStore(config, new TestSpecSerDe());
  }

  @Test
  public void testAddSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec1);
    this.specStore.addSpec(this.flowSpec2);

    Assert.assertTrue(this.specStore.exists(this.uri1));
    Assert.assertTrue(this.specStore.exists(this.uri2));
    Assert.assertFalse(this.specStore.exists(URI.create("dummy")));
  }

  @Test
  public void testGetSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec1);
    this.specStore.addSpec(this.flowSpec2);

    FlowSpec result = (FlowSpec) this.specStore.getSpec(this.uri1);
    Assert.assertEquals(result, this.flowSpec1);

    Collection<Spec> specs = this.specStore.getSpecs();
    Assert.assertTrue(specs.contains(this.flowSpec1));
    Assert.assertTrue(specs.contains(this.flowSpec2));

    Iterator<URI> uris = this.specStore.getSpecURIs();
    Assert.assertTrue(Iterators.contains(uris, this.uri1));
    Assert.assertTrue(Iterators.contains(uris, this.uri2));
  }

  @Test
  public void testDeleteSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec1);
    Assert.assertTrue(this.specStore.exists(this.uri1));

    this.specStore.deleteSpec(this.uri1);
    Assert.assertFalse(this.specStore.exists(this.uri1));
  }

  public class TestSpecSerDe implements SpecSerDe {
    @Override
    public byte[] serialize(Spec spec) {
      return SerializationUtils.serialize(spec);
    }

    @Override
    public Spec deserialize(byte[] spec) {
      return SerializationUtils.deserialize(spec);
    }
  }
}