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

package org.apache.gobblin.service.modules.restli;

import java.io.IOException;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.linkedin.data.template.RequiredFieldNotPresentException;
import com.linkedin.data.template.StringMap;

import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigLoggedException;
import org.apache.gobblin.service.FlowConfigResourceLocalHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.Schedule;


@Test
public class FlowConfigUtilsTest {
  private void testFlowSpec(FlowConfig flowConfig) {
    try {
      FlowConfigResourceLocalHandler.createFlowSpecForConfig(flowConfig);
    } catch (FlowConfigLoggedException e) {
      Assert.fail("Should not get to here");
    }
  }

  private void testSerDer(FlowConfig flowConfig) {
    try {
      String serialized = FlowConfigUtils.serializeFlowConfig(flowConfig);
      FlowConfig newFlowConfig = FlowConfigUtils.deserializeFlowConfig(serialized);
      Assert.assertTrue(testEqual(flowConfig, newFlowConfig));
    } catch (IOException e) {
      Assert.fail("Should not get to here");
    }
  }

  /**
   * Due to default value setting, flow config after deserialization might contain default value.
   * Only check f1.equals(f2) is not enough
   */
  private boolean testEqual(FlowConfig f1, FlowConfig f2) {
    if (f1.equals(f2)) {
      return true;
    }

    // Check Id
    Assert.assertTrue(f1.hasId() == f2.hasId());
    Assert.assertTrue(f1.getId().equals(f2.getId()));

    // Check Schedule
    Assert.assertTrue(f1.hasSchedule() == f2.hasSchedule());
    if (f1.hasSchedule()) {
       Schedule s1 = f1.getSchedule();
       Schedule s2 = f2.getSchedule();
       Assert.assertTrue(s1.getCronSchedule().equals(s2.getCronSchedule()));
       Assert.assertTrue(s1.isRunImmediately().equals(s2.isRunImmediately()));
    }

    // Check Template URI
    Assert.assertTrue(f1.hasTemplateUris() == f2.hasTemplateUris());
    if (f1.hasTemplateUris()) {
      Assert.assertTrue(f1.getTemplateUris().equals(f2.getTemplateUris()));
    }

    // Check Properties
    Assert.assertTrue(f1.hasProperties() == f2.hasProperties());
    if (f1.hasProperties()) {
      Assert.assertTrue(f1.getProperties().equals(f2.getProperties()));
    }

    return true;
  }

  public void testFullFlowConfig() {
    FlowConfig flowConfig = new FlowConfig().setId(new FlowId()
        .setFlowName("SN_CRMSYNC")
        .setFlowGroup("DYNAMICS-USER-123456789"));
    flowConfig.setSchedule(new Schedule()
        .setCronSchedule("0 58 2/12 ? * * *")
        .setRunImmediately(Boolean.valueOf("true")));

    flowConfig.setTemplateUris("FS:///my.template");
    Properties properties = new Properties();
    properties.put("gobblin.flow.sourceIdentifier", "dynamicsCrm");
    properties.put("gobblin.flow.destinationIdentifier", "espresso");
    flowConfig.setProperties(new StringMap(Maps.fromProperties(properties)));

    testFlowSpec(flowConfig);
    testSerDer(flowConfig);
  }

  public void testFlowConfigWithoutSchedule() {
    FlowConfig flowConfig = new FlowConfig().setId(new FlowId()
        .setFlowName("SN_CRMSYNC")
        .setFlowGroup("DYNAMICS-USER-123456789"));

    flowConfig.setTemplateUris("FS:///my.template");
    Properties properties = new Properties();
    properties.put("gobblin.flow.sourceIdentifier", "dynamicsCrm");
    properties.put("gobblin.flow.destinationIdentifier", "espresso");
    flowConfig.setProperties(new StringMap(Maps.fromProperties(properties)));

    testFlowSpec(flowConfig);
    testSerDer(flowConfig);
  }

  public void testFlowConfigWithDefaultRunImmediately() {
    FlowConfig flowConfig = new FlowConfig().setId(new FlowId()
        .setFlowName("SN_CRMSYNC")
        .setFlowGroup("DYNAMICS-USER-123456789"));
    flowConfig.setSchedule(new Schedule()
        .setCronSchedule("0 58 2/12 ? * * *"));

    flowConfig.setTemplateUris("FS:///my.template");
    Properties properties = new Properties();
    properties.put("gobblin.flow.sourceIdentifier", "dynamicsCrm");
    properties.put("gobblin.flow.destinationIdentifier", "espresso");
    flowConfig.setProperties(new StringMap(Maps.fromProperties(properties)));

    testFlowSpec(flowConfig);
    testSerDer(flowConfig);
  }

  public void testFlowConfigWithoutTemplateUri() {
    FlowConfig flowConfig = new FlowConfig().setId(new FlowId()
        .setFlowName("SN_CRMSYNC")
        .setFlowGroup("DYNAMICS-USER-123456789"));
    flowConfig.setSchedule(new Schedule()
        .setCronSchedule("0 58 2/12 ? * * *"));

    Properties properties = new Properties();
    properties.put("gobblin.flow.sourceIdentifier", "dynamicsCrm");
    properties.put("gobblin.flow.destinationIdentifier", "espresso");
    flowConfig.setProperties(new StringMap(Maps.fromProperties(properties)));

    try {
      FlowConfigResourceLocalHandler.createFlowSpecForConfig(flowConfig);
      Assert.fail("Should not get to here");
    } catch (RequiredFieldNotPresentException e) {
      Assert.assertTrue(true, "templateUri cannot be empty");
    }
    testSerDer(flowConfig);
  }
}
