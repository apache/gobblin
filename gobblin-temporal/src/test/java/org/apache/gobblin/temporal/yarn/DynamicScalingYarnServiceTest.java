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

package org.apache.gobblin.temporal.yarn;

import java.net.URL;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;

/** Tests for {@link DynamicScalingYarnService} */
public class DynamicScalingYarnServiceTest {
  private Config defaultConfigs;
  private final YarnConfiguration yarnConfiguration = new YarnConfiguration();
  private final FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
  private final EventBus eventBus = new EventBus("TemporalDynamicScalingYarnServiceTest");

  @BeforeClass
  public void setup() {
    URL url = DynamicScalingYarnServiceTest.class.getClassLoader()
        .getResource(YarnServiceTest.class.getSimpleName() + ".conf"); // using same initial config as of YarnServiceTest
    Assert.assertNotNull(url, "Could not find resource " + url);
    this.defaultConfigs = ConfigFactory.parseURL(url).resolve();
  }

  @Test
  public void testReviseWorkforcePlanAndRequestNewContainers() throws Exception {
    int numNewContainers = 5;
    DynamicScalingYarnService dynamicScalingYarnService = new DynamicScalingYarnService(this.defaultConfigs, "testApp", "testAppId", yarnConfiguration, mockFileSystem, eventBus);
    DynamicScalingYarnService dynamicScalingYarnServiceSpy = Mockito.spy(dynamicScalingYarnService);
    Mockito.doNothing().when(dynamicScalingYarnServiceSpy).requestContainers(Mockito.anyInt(), Mockito.any(Resource.class), Mockito.any(Optional.class));
    ScalingDirective baseScalingDirective = new ScalingDirective(WorkforceProfiles.BASELINE_NAME, numNewContainers, System.currentTimeMillis());
    dynamicScalingYarnServiceSpy.reviseWorkforcePlanAndRequestNewContainers(Collections.singletonList(baseScalingDirective));
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(1)).requestContainers(Mockito.eq(numNewContainers), Mockito.any(Resource.class), Mockito.any(Optional.class));
  }
}
