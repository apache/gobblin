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

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.hadoop.yarn.api.records.Container;
import java.net.URL;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Tests for {@link YarnService}*/
public class YarnServiceTest {
  private Config defaultConfigs;
  private final YarnConfiguration yarnConfiguration = new YarnConfiguration();
  private final FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
  private final EventBus eventBus = new EventBus("TemporalYarnServiceTest");

  @BeforeClass
  public void setup() {
    URL url = YarnServiceTest.class.getClassLoader()
        .getResource(YarnServiceTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);
    this.defaultConfigs = ConfigFactory.parseURL(url).resolve();
  }

  @Test
  public void testBaselineWorkerProfileCreatedWithPassedConfigs() throws Exception {
    final int containerMemoryMbs = 1500;
    final int containerCores = 5;
    final int numContainers = 4;
    Config config = this.defaultConfigs
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY, ConfigValueFactory.fromAnyRef(containerMemoryMbs))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY, ConfigValueFactory.fromAnyRef(containerCores))
        .withValue(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY, ConfigValueFactory.fromAnyRef(numContainers));

    YarnService yarnService = new YarnService(
        config,
        "testApplicationName",
        "testApplicationId",
        yarnConfiguration,
        mockFileSystem,
        eventBus
    );

    Assert.assertEquals(yarnService.baselineWorkerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY), containerMemoryMbs);
    Assert.assertEquals(yarnService.baselineWorkerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY), containerCores);
    Assert.assertEquals(yarnService.baselineWorkerProfile.getConfig().getInt(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY), numContainers);
  }

  @Test
  public void testBuildContainerCommand() throws Exception {
    final double jvmMemoryXmxRatio = 0.7;
    final int jvmMemoryOverheadMbs = 50;
    final int resourceMemoryMB = 3072;
    final int expectedJvmMemory = (int) (resourceMemoryMB * jvmMemoryXmxRatio) - jvmMemoryOverheadMbs;

    Config config = this.defaultConfigs.withValue(GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY, ConfigValueFactory.fromAnyRef(jvmMemoryXmxRatio))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY, ConfigValueFactory.fromAnyRef(jvmMemoryOverheadMbs));

    Resource resource = Resource.newInstance(resourceMemoryMB, 2);

    Container mockContainer = Mockito.mock(Container.class);
    Mockito.when(mockContainer.getResource()).thenReturn(resource);
    Mockito.when(mockContainer.getAllocationRequestId()).thenReturn(0L);

    YarnService yarnService = new YarnService(
        config,
        "testApplicationName",
        "testApplicationId",
        yarnConfiguration,
        mockFileSystem,
        eventBus
    );

    String command = yarnService.buildContainerCommand(mockContainer, "testHelixParticipantId", "testHelixInstanceTag");
    Assert.assertTrue(command.contains("-Xmx" + expectedJvmMemory + "M"));
  }
}
