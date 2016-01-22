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

package gobblin.config.client;

import java.net.URI;
import java.net.URISyntaxException;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.config.common.impl.SingleLinkedListConfigKeyPath;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;

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
    
    URI defaultRootURI = ConfigClientUtils.getDefaultRootURI(clientRelativeURI, mockConfigStore);
    Assert.assertEquals(defaultRootURI.toString(), "etl-hdfs:/");
    
    ConfigKeyPath configKey = SingleLinkedListConfigKeyPath.ROOT.createChild("data").createChild("databases").createChild("Identity");
    URI absURI = ConfigClientUtils.getAbsoluteURI(configKey, mockConfigStore);
    Assert.assertEquals(absURI.toString(), 
        "etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/data/databases/Identity");
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