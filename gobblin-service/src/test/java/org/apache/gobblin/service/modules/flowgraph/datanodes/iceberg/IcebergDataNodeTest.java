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

package org.apache.gobblin.service.modules.flowgraph.datanodes.iceberg;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;


public class IcebergDataNodeTest {

  Config config = null;

  @BeforeMethod
  public void setUp() {
    String sampleNodeId = "some-iceberg-node-id";
    String sampleAdlFsUri = "hdfs://data.hdfs.core.windows.net";
    String sampleCatalogUri = "https://xyz.company.com/clusters/db/catalog:443";

    config = ConfigFactory.empty()
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(sampleNodeId))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(sampleAdlFsUri))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "iceberg.catalog.uri", ConfigValueFactory.fromAnyRef(sampleCatalogUri));
  }

  @AfterMethod
  public void tearDown() {
  }
  @Test
  public void testIcebergDataNodeWithValidCatalogUri() throws DataNode.DataNodeCreationException {
    IcebergDataNode icebergDataNode = new IcebergDataNode(config);
    Assert.assertNotNull(icebergDataNode);
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIcebergDataNodeWithInvalidCatalogUri() throws DataNode.DataNodeCreationException {
    String emptyCatalogUri = "";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "iceberg.catalog.uri", ConfigValueFactory.fromAnyRef(emptyCatalogUri));
    IcebergDataNode icebergDataNode = new IcebergDataNode(config);
  }
}
