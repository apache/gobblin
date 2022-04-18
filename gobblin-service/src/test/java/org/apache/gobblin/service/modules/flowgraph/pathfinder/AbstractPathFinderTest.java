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

package org.apache.gobblin.service.modules.flowgraph.pathfinder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.restli.FlowConfigUtils;


public class AbstractPathFinderTest {

  @Test
  public void convertDataNodesTest() {
    Config flowConfig = ConfigFactory.empty()
        .withValue(ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, ConfigValueFactory.fromAnyRef("node1-alpha,node2"));
    Map<String, String> dataNodeAliasMap = new HashMap<>();
    dataNodeAliasMap.put("node1-alpha", "node1");
    dataNodeAliasMap.put("node1-beta", "node1");
    dataNodeAliasMap.put("node3-alpha", "node3");
    dataNodeAliasMap.put("node1-beta", "node3");

    List<String> dataNodes = FlowConfigUtils.getDataNodes(flowConfig, ServiceConfigKeys.FLOW_DESTINATION_IDENTIFIER_KEY, dataNodeAliasMap);
    Assert.assertEquals(dataNodes.size(), 2);
    Assert.assertTrue(dataNodes.contains("node1"));
    Assert.assertTrue(dataNodes.contains("node2"));
  }
}
