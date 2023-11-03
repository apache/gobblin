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

package org.apache.gobblin.runtime.api;

import com.typesafe.config.Config;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.FlowId;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.gobblin.runtime.api.FlowSpec.*;


public class FlowSpecTest {

  /**
   * Tests that the addProperty() function to ensure the new flowSpec returned has the original properties and updated
   * ones
   * @throws URISyntaxException
   */
  @Test
  public void testAddProperty() throws URISyntaxException {
    String flowGroup = "myGroup";
    String flowName = "myName";
    String flowExecutionId = "myId";
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    URI flowUri = FlowSpec.Utils.createFlowSpecUri(flowId);

    // Create properties to be used as config
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup);
    properties.setProperty(ConfigurationKeys.FLOW_NAME_KEY, flowName);
    properties.setProperty(ConfigurationKeys.FLOW_IS_REMINDER_EVENT_KEY, "true");

    FlowSpec originalFlowSpec = FlowSpec.builder(flowUri).withConfigAsProperties(properties).build();
    FlowSpec updatedFlowSpec = createFlowSpecWithProperty(originalFlowSpec, ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);

    Properties updatedProperties = updatedFlowSpec.getConfigAsProperties();
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), flowExecutionId);
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_GROUP_KEY), flowGroup);
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_NAME_KEY), flowName);
    Assert.assertEquals(updatedProperties.getProperty(ConfigurationKeys.FLOW_IS_REMINDER_EVENT_KEY), "true");

    Config updatedConfig = updatedFlowSpec.getConfig();
    Assert.assertEquals(updatedConfig.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), flowExecutionId);
    Assert.assertEquals(updatedConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY), flowGroup);
    Assert.assertEquals(updatedConfig.getString(ConfigurationKeys.FLOW_NAME_KEY), flowName);
    Assert.assertEquals(updatedConfig.getString(ConfigurationKeys.FLOW_IS_REMINDER_EVENT_KEY), "true");
  }
}
