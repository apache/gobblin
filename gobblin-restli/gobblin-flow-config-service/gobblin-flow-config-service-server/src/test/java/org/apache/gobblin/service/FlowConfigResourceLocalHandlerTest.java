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

package org.apache.gobblin.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.linkedin.data.template.StringMap;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;


public class FlowConfigResourceLocalHandlerTest {
  private static final String TEST_GROUP_NAME = "testGroup1";
  private static final String TEST_FLOW_NAME = "testFlow1";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

  @Test
  public void testCreateFlowSpecForConfig() throws URISyntaxException {
    Map<String, String> flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "a:b:c*.d");

    FlowConfig flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));

    FlowSpec flowSpec = FlowConfigResourceLocalHandler.createFlowSpecForConfig(flowConfig);
    Assert.assertEquals(flowSpec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), TEST_GROUP_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(ConfigurationKeys.JOB_SCHEDULE_KEY), TEST_SCHEDULE);
    Assert.assertEquals(flowSpec.getConfig().getBoolean(ConfigurationKeys.FLOW_RUN_IMMEDIATELY), true);
    Assert.assertEquals(flowSpec.getConfig().getString("param1"), "a:b:c*.d");
    Assert.assertEquals(flowSpec.getTemplateURIs().get().size(), 1);
    Assert.assertTrue(flowSpec.getTemplateURIs().get().contains(new URI(TEST_TEMPLATE_URI)));

    flowProperties = Maps.newHashMap();
    flowProperties.put("param1", "value1");
    flowProperties.put("param2", "${param1}-123");
    flowProperties.put("param3", "\"a:b:c*.d\"");
    flowConfig = new FlowConfig().setId(new FlowId().setFlowGroup(TEST_GROUP_NAME).setFlowName(TEST_FLOW_NAME))
        .setTemplateUris(TEST_TEMPLATE_URI).setSchedule(new Schedule().setCronSchedule(TEST_SCHEDULE).
            setRunImmediately(true))
        .setProperties(new StringMap(flowProperties));
    flowSpec = FlowConfigResourceLocalHandler.createFlowSpecForConfig(flowConfig);

    Assert.assertEquals(flowSpec.getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY), TEST_GROUP_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY), TEST_FLOW_NAME);
    Assert.assertEquals(flowSpec.getConfig().getString(ConfigurationKeys.JOB_SCHEDULE_KEY), TEST_SCHEDULE);
    Assert.assertEquals(flowSpec.getConfig().getBoolean(ConfigurationKeys.FLOW_RUN_IMMEDIATELY), true);
    Assert.assertEquals(flowSpec.getConfig().getString("param1"),"value1");
    Assert.assertEquals(flowSpec.getConfig().getString("param2"),"value1-123");
    Assert.assertEquals(flowSpec.getConfig().getString("param3"), "a:b:c*.d");
    Assert.assertEquals(flowSpec.getTemplateURIs().get().size(), 1);
    Assert.assertTrue(flowSpec.getTemplateURIs().get().contains(new URI(TEST_TEMPLATE_URI)));
  }
}