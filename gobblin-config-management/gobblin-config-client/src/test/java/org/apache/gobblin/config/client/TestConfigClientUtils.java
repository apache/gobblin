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

package org.apache.gobblin.config.client;

import java.net.URI;
import java.net.URISyntaxException;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.apache.gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import org.apache.gobblin.config.store.api.ConfigKeyPath;
import org.apache.gobblin.config.store.api.ConfigStore;

@Test(groups = { "gobblin.config.client.api" })
public class TestConfigClientUtils {
  private ConfigStore mockConfigStore;
  private final String version = "V1.0";

  @BeforeClass
  public void setup() throws Exception{
    mockConfigStore = mock(ConfigStore.class, Mockito.RETURNS_SMART_NULLS);
    URI configStorURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest");

    when(mockConfigStore.getCurrentVersion()).thenReturn(version);
    when(mockConfigStore.getStoreURI()).thenReturn(configStorURI);
  }

  @Test
  public void testGetConfigKeyPath() throws URISyntaxException{
    String expected = "/datasets/a1/a2";
    URI clientAbsURI = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/datasets/a1/a2");
    ConfigKeyPath result = ConfigClientUtils.buildConfigKeyPath(clientAbsURI, mockConfigStore);
    Assert.assertEquals(result.toString(), expected);

    URI clientRelativeURI = new URI("etl-hdfs:///datasets/a1/a2");
    result = ConfigClientUtils.buildConfigKeyPath(clientRelativeURI, mockConfigStore);
    Assert.assertEquals(result.toString(), expected);

    clientRelativeURI = new URI("etl-hdfs:/datasets/a1/a2");
    result = ConfigClientUtils.buildConfigKeyPath(clientRelativeURI, mockConfigStore);
    Assert.assertEquals(result.toString(), expected);

    ConfigKeyPath configKey = SingleLinkedListConfigKeyPath.ROOT.createChild("data").createChild("databases").createChild("Identity");
    // client app pass URI without authority
    URI adjusted = ConfigClientUtils.buildUriInClientFormat(configKey, mockConfigStore, false);
    Assert.assertTrue(adjusted.toString().equals("etl-hdfs:/data/databases/Identity"));

    // client app pass URI with authority
    adjusted = ConfigClientUtils.buildUriInClientFormat(configKey, mockConfigStore, true);
    Assert.assertTrue(adjusted.toString().equals("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/data/databases/Identity"));
  }

  @Test
  public void testIsAncestorOrSame() throws Exception{
    //Path ancestor = new Path("/");
    //Path descendant = new Path("/");

    URI ancestor = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/");
    URI descendant = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/datasets/a1/a2");

    Assert.assertTrue(ConfigClientUtils.isAncestorOrSame(descendant, ancestor));

    // ends with "/"
    descendant = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/datasets/a1/a2/");
    Assert.assertTrue(ConfigClientUtils.isAncestorOrSame(descendant, ancestor));

    // wrong authority
    descendant = new URI("etl-hdfs://ltx1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/datasets/a1/a2");
    Assert.assertTrue(!ConfigClientUtils.isAncestorOrSame(descendant, ancestor));

    // wrong path
    descendant = new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/sahil/HdfsBasedConfigTest/datasets/a1/a2");
    Assert.assertTrue(!ConfigClientUtils.isAncestorOrSame(descendant, ancestor));

    ConfigKeyPath data = SingleLinkedListConfigKeyPath.ROOT.createChild("data");
    ConfigKeyPath data2 = SingleLinkedListConfigKeyPath.ROOT.createChild("data2");
    ConfigKeyPath identity = SingleLinkedListConfigKeyPath.ROOT.createChild("data").createChild("databases").createChild("Identity");
    Assert.assertTrue(ConfigClientUtils.isAncestorOrSame(identity, data));
    Assert.assertTrue(ConfigClientUtils.isAncestorOrSame(identity, SingleLinkedListConfigKeyPath.ROOT));
    Assert.assertTrue(!ConfigClientUtils.isAncestorOrSame(identity, data2));
  }

  @Test (expectedExceptions = java.lang.IllegalArgumentException.class )
  public void testInvalidSchemeURI() throws URISyntaxException {
    URI clientURI = new URI("hdfs:///datasets/a1/a2");
    ConfigClientUtils.buildConfigKeyPath(clientURI, mockConfigStore);
  }

  @Test (expectedExceptions = java.lang.IllegalArgumentException.class )
  public void testInvalidAuthortiyURI() throws URISyntaxException {
    URI clientURI = new URI("etl-hdfs://ltx1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/datasets/a1/a2");
    ConfigClientUtils.buildConfigKeyPath(clientURI, mockConfigStore);
  }
}