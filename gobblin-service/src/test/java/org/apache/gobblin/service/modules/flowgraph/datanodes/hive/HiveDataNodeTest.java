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

package org.apache.gobblin.service.modules.flowgraph.datanodes.hive;

import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;

import org.junit.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;


public class HiveDataNodeTest {

  Config config = null;

  @BeforeMethod
  public void setUp() {
    String expectedNodeId = "some-node-id";
    String expectedAdlFsUri = "abfs://data@adl.dfs.core.windows.net";
    String expectedHiveMetastoreUri = "thrift://hcat.company.com:7552";

    config = ConfigFactory.empty()
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(expectedNodeId))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "fs.uri", ConfigValueFactory.fromAnyRef(expectedAdlFsUri))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri", ConfigValueFactory.fromAnyRef(expectedHiveMetastoreUri));
  }

  @AfterMethod
  public void tearDown() {
  }


  @Test
  public void testIsMetastoreUriValid() throws Exception {
    HiveDataNode hiveDataNode = new HiveDataNode(config);
    Assert.assertNotNull(hiveDataNode);
  }

  @Test(expectedExceptions = DataNode.DataNodeCreationException.class)
  public void testIsMetastoreUriInValid() throws Exception {
    String expectedHiveMetastoreUri = "thrift-1://hcat.company.com:7552";
    config = config.withValue(FlowGraphConfigurationKeys.DATA_NODE_PREFIX + "hive.metastore.uri", ConfigValueFactory.fromAnyRef(expectedHiveMetastoreUri));
    HiveDataNode hiveDataNode = new HiveDataNode(config);
  }

}