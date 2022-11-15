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

import java.net.URI;
import java.net.URISyntaxException;

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
    String sampleHiveMetastoreUri = "thrift://hcat.company.com:7552";

    config = ConfigFactory.empty()
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(sampleNodeId))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(sampleAdlFsUri))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri", ConfigValueFactory.fromAnyRef(sampleHiveMetastoreUri));
  }

  @AfterMethod
  public void tearDown() {
  }

  @Test
  public void testIcebergDataNodeWithValidMetastoreUri() throws DataNode.DataNodeCreationException, URISyntaxException {
    IcebergOnHiveDataNode icebergDataNode = new IcebergOnHiveDataNode(config);
    URI uri = new URI(config.getString(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri"));
    Assert.assertTrue(icebergDataNode.isMetastoreUriValid(uri));
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIcebergDataNodeWithInvalidMetastoreUri() throws DataNode.DataNodeCreationException, URISyntaxException {
    String bogusHiveMetastoreUri = "not-thrift://hcat.company.com:7552";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri", ConfigValueFactory.fromAnyRef(bogusHiveMetastoreUri));
    IcebergOnHiveDataNode icebergDataNode = new IcebergOnHiveDataNode(config);
    URI uri = new URI(config.getString(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri"));
    icebergDataNode.isMetastoreUriValid(uri);
  }
}
