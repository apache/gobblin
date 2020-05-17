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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
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
import org.apache.gobblin.runtime.api.SpecSerDeException;
import org.apache.gobblin.runtime.spec_serde.GsonFlowSpecSerDe;


public class MysqlSpecStoreTest {
  private static final String USER = "testUser";
  private static final String PASSWORD = "testPassword";
  private static final String TABLE = "spec_store";

  private MysqlSpecStore specStore;
  private MysqlSpecStore oldSpecStore;
  private URI uri1 = URI.create("flowspec1");
  private URI uri2 = URI.create("flowspec2");
  private URI uri3 = URI.create("flowspec3");
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
  private FlowSpec flowSpec3 = FlowSpec.builder(this.uri3)
      .withConfig(ConfigBuilder.create().addPrimitive("key3", "value3").build())
      .withDescription("Test flow spec 3")
      .withVersion("Test version 3")
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
    this.oldSpecStore = new OldSpecStore(config, new TestSpecSerDe());
  }

  @Test
  public void testAddSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec1);
    this.specStore.addSpec(this.flowSpec2);

    Assert.assertTrue(this.specStore.exists(this.uri1));
    Assert.assertTrue(this.specStore.exists(this.uri2));
    Assert.assertFalse(this.specStore.exists(URI.create("dummy")));
  }

  @Test (dependsOnMethods = "testReadOldColumn")
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
  public void testGetSpecWithTag() throws Exception {

    //Creating and inserting flowspecs with tags
    URI uri4 = URI.create("flowspec4");
    FlowSpec flowSpec4 = FlowSpec.builder(uri4)
        .withConfig(ConfigBuilder.create().addPrimitive("key4", "value4").build())
        .withDescription("Test flow spec 4")
        .withVersion("Test version 4")
        .build();

    URI uri5 = URI.create("flowspec5");
    FlowSpec flowSpec5 = FlowSpec.builder(uri5)
        .withConfig(ConfigBuilder.create().addPrimitive("key5", "value5").build())
        .withDescription("Test flow spec 5")
        .withVersion("Test version 5")
        .build();

    this.specStore.addSpec(flowSpec4, "dr");
    this.specStore.addSpec(flowSpec5, "dr");

    Assert.assertTrue(this.specStore.exists(uri4));
    Assert.assertTrue(this.specStore.exists(uri5));
    List<URI> result = new ArrayList<>();
    this.specStore.getSpecURIsWithTag("dr").forEachRemaining(result::add);
    Assert.assertEquals(result.size(), 2);
  }

  @Test (expectedExceptions = {IOException.class})
  public void testGetCorruptedSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec3);
  }

  @Test (dependsOnMethods = "testGetSpec")
  public void testDeleteSpec() throws Exception {
    this.specStore.addSpec(this.flowSpec1);
    Assert.assertTrue(this.specStore.exists(this.uri1));

    this.specStore.deleteSpec(this.uri1);
    Assert.assertFalse(this.specStore.exists(this.uri1));
  }

  @Test (dependsOnMethods = "testAddSpec")
  public void testReadOldColumn() throws Exception {
    this.oldSpecStore.addSpec(this.flowSpec1);

    FlowSpec spec = (FlowSpec) this.specStore.getSpec(this.uri1);
    Assert.assertEquals(spec, this.flowSpec1);
  }

  /**
   * A {@link MysqlSpecStore} which does not write into the new spec_json column
   * to simulate behavior of a table with old data.
   */
  public static class OldSpecStore extends MysqlSpecStore {

    public OldSpecStore(Config config, SpecSerDe specSerDe) throws IOException {
      super(config, specSerDe);
    }

    @Override
    public void addSpec(Spec spec, String tagValue) throws IOException {
      try (Connection connection = this.dataSource.getConnection();
          PreparedStatement statement = connection.prepareStatement(String.format(INSERT_STATEMENT, this.tableName))) {
        statement.setString(1, spec.getUri().toString());
        statement.setString(2, tagValue);
        statement.setBlob(3, new ByteArrayInputStream(this.specSerDe.serialize(spec)));
        statement.setString(4, null);
        statement.executeUpdate();
        connection.commit();
      } catch (SQLException | SpecSerDeException e) {
        throw new IOException(e);
      }
    }
  }

  public class TestSpecSerDe extends GsonFlowSpecSerDe {
    @Override
    public byte[] serialize(Spec spec) throws SpecSerDeException {
      byte[] bytes = super.serialize(spec);
      // Reverse bytes to simulate corrupted Spec
      if (spec.getUri().equals(uri3)) {
        ArrayUtils.reverse(bytes);
      }
      return bytes;
    }
  }
}