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
package org.apache.gobblin.service.modules.flowgraph.datanodes.fs;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.dataset.FSDatasetDescriptor;
import org.apache.gobblin.service.modules.flowgraph.DataNode;
import org.apache.gobblin.service.modules.flowgraph.FlowGraphConfigurationKeys;


public class SftpDataNodeTest {

  @Test
  public void testCreate() throws DataNode.DataNodeCreationException {
    //Create a SFTP DataNode with default SFTP port
    Config config = ConfigFactory.empty().withValue(SftpDataNode.SFTP_HOSTNAME,
        ConfigValueFactory.fromAnyRef("testHost"))
        .withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY, ConfigValueFactory.fromAnyRef("testId"));
    SftpDataNode dataNode = new SftpDataNode(config);
    Assert.assertEquals(dataNode.getId(), "testId");
    Assert.assertEquals(dataNode.getHostName(), "testHost");
    Assert.assertEquals(dataNode.getPort().intValue(), ConfigurationKeys.SOURCE_CONN_DEFAULT_PORT);
    Assert.assertEquals(dataNode.getDefaultDatasetDescriptorPlatform(), SftpDataNode.PLATFORM);
    Assert.assertEquals(dataNode.getDefaultDatasetDescriptorClass(), FSDatasetDescriptor.class.getCanonicalName());

    config = config.withValue(SftpDataNode.SFTP_PORT, ConfigValueFactory.fromAnyRef(143));
    SftpDataNode dataNodeWithPort = new SftpDataNode(config);
    Assert.assertEquals(dataNode.getId(), "testId");
    Assert.assertEquals(dataNode.getHostName(), "testHost");
    Assert.assertEquals(dataNodeWithPort.getPort().intValue(), 143);
    Assert.assertEquals(dataNode.getDefaultDatasetDescriptorPlatform(), SftpDataNode.PLATFORM);
    Assert.assertEquals(dataNode.getDefaultDatasetDescriptorClass(), FSDatasetDescriptor.class.getCanonicalName());

    Config configMissingProps = ConfigFactory.empty().withValue(FlowGraphConfigurationKeys.DATA_NODE_ID_KEY,
        ConfigValueFactory.fromAnyRef("testId"));
    try {
      DataNode sftpNode = new SftpDataNode(configMissingProps);
      Assert.fail("Unexpected success in creating Sftp node.");
    } catch (DataNode.DataNodeCreationException e) {
      //Expected exception.
    }
  }
}