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

package org.apache.gobblin.service.modules.flowgraph.datanodes;

import org.junit.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

public class HttpDataNodeTest {

  @Test
  public void testConfig() throws DataNode.DataNodeCreationException {
    String expectedNodeId = "some-node-id";
    String expectedHttpDomain = "https://a.b.c";
    String expectedHttpAuthType = "oauth";

    Config config = ConfigFactory.empty()
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(expectedNodeId))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_HTTP_DOMAIN_KEY, ConfigValueFactory.fromAnyRef(expectedHttpDomain))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_HTTP_AUTHENTICATION_TYPE_KEY, ConfigValueFactory.fromAnyRef(expectedHttpAuthType));
    HttpDataNode node = new HttpDataNode(config);

    // Verify the node id
    String id = node.getId();
    Assert.assertTrue(id.equals(expectedNodeId));

    Config rawConfig = node.getRawConfig();
    String httpDomain = ConfigUtils.getString(rawConfig, FlowGraphConfigurationKeys.DATA_NODE_HTTP_DOMAIN_KEY, "");
    String httpAuthType = ConfigUtils.getString(rawConfig, FlowGraphConfigurationKeys.DATA_NODE_HTTP_AUTHENTICATION_TYPE_KEY, "");
    // Verify config saved to the node successfully
    Assert.assertTrue(httpDomain.equals(expectedHttpDomain));
    Assert.assertTrue(httpAuthType.equals(expectedHttpAuthType));

  }
}