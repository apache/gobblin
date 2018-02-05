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
package org.apache.gobblin.service.modules.scheduler;

import java.net.URI;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpec;


public class GobblinServiceJobSchedulerTest {
  private static final String TEST_GROUP_NAME = "testGroup";
  private static final String TEST_FLOW_NAME = "testFlow";
  private static final String TEST_SCHEDULE = "0 1/0 * ? * *";
  private static final String TEST_TEMPLATE_URI = "FS:///templates/test.template";

  @Test
  public void testDisableFlowRunImmediatelyOnStart()
      throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationKeys.FLOW_RUN_IMMEDIATELY, "true");
    properties.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, TEST_SCHEDULE);
    properties.setProperty(ConfigurationKeys.JOB_GROUP_KEY, TEST_GROUP_NAME);
    properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, TEST_FLOW_NAME);
    Config config = ConfigFactory.parseProperties(properties);
    FlowSpec spec = FlowSpec.builder().withTemplate(new URI(TEST_TEMPLATE_URI)).withVersion("version")
        .withConfigAsProperties(properties).withConfig(config).build();
    FlowSpec modifiedSpec = (FlowSpec) GobblinServiceJobScheduler.disableFlowRunImmediatelyOnStart(spec);
    for (URI templateURI : modifiedSpec.getTemplateURIs().get()) {
      Assert.assertEquals(templateURI.toString(), TEST_TEMPLATE_URI);
    }
    Assert.assertEquals(modifiedSpec.getVersion(), "version");
    Config modifiedConfig = modifiedSpec.getConfig();
    Assert.assertFalse(modifiedConfig.getBoolean(ConfigurationKeys.FLOW_RUN_IMMEDIATELY));
    Assert.assertEquals(modifiedConfig.getString(ConfigurationKeys.JOB_GROUP_KEY), TEST_GROUP_NAME);
    Assert.assertEquals(modifiedConfig.getString(ConfigurationKeys.JOB_NAME_KEY), TEST_FLOW_NAME);
  }
}