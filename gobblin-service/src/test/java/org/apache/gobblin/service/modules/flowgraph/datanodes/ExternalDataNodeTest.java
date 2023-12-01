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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.gobblin.service.modules.dataset.ExternalUriDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;
import org.junit.Assert;
import org.testng.annotations.Test;


public class ExternalDataNodeTest {

  @Test
  public void testConfig() throws DataNode.DataNodeCreationException {
    String expectedNodeId = "some-node-id";

    Config config = ConfigFactory.empty()
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef(expectedNodeId));
    ExternalUriDataNode node = new ExternalUriDataNode(config);

    // Verify the node id
    String id = node.getId();
    Assert.assertEquals(id, expectedNodeId);
    Assert.assertEquals(node.getDefaultDatasetDescriptorPlatform(), "external");
    Assert.assertEquals(node.getDefaultDatasetDescriptorClass(), ExternalUriDatasetDescriptor.class.getCanonicalName());
  }
}