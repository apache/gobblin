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

import java.io.IOException;
import java.net.URL;

import org.apache.gobblin.temporal.dynamic.WorkerProfile;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.base.Optional;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import com.google.common.eventbus.EventBus;

import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;

import static org.mockito.Mockito.*;


/**
 * Tests for {@link YarnService}
 *
 * NOTE : This test is a partial clone of {@link org.apache.gobblin.yarn.YarnServiceTest}
 * */
public class YarnServiceTest {
  private Config defaultConfigs;
  private final YarnConfiguration yarnConfiguration = new YarnConfiguration();
  private final FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
  private final EventBus eventBus = new EventBus("TemporalYarnServiceTest");
  private AMRMClientAsync mockAMRMClient;
  private RegisterApplicationMasterResponse mockRegisterApplicationMasterResponse;

  @BeforeClass
  public void setup() throws IOException, YarnException {
    mockAMRMClient = mock(AMRMClientAsync.class);
    mockRegisterApplicationMasterResponse = mock(RegisterApplicationMasterResponse.class);

    URL url = YarnServiceTest.class.getClassLoader()
        .getResource(YarnServiceTest.class.getSimpleName() + ".conf");
    Assert.assertNotNull(url, "Could not find resource " + url);
    this.defaultConfigs = ConfigFactory.parseURL(url).resolve();

    MockedStatic<AMRMClientAsync> amrmClientAsyncMockStatic = mockStatic(AMRMClientAsync.class);

    amrmClientAsyncMockStatic.when(() -> AMRMClientAsync.createAMRMClientAsync(anyInt(), any(AMRMClientAsync.CallbackHandler.class)))
        .thenReturn(mockAMRMClient);
    doNothing().when(mockAMRMClient).init(any(YarnConfiguration.class));

    when(mockAMRMClient.registerApplicationMaster(anyString(), anyInt(), anyString()))
        .thenReturn(mockRegisterApplicationMasterResponse);
    when(mockRegisterApplicationMasterResponse.getMaximumResourceCapability())
        .thenReturn(Mockito.mock(Resource.class));
  }

  @Test
  public void testYarnServiceStartupWithInitialContainers() throws Exception {
    int expectedNumContainers = 3;
    Config config = this.defaultConfigs.withValue(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY, ConfigValueFactory.fromAnyRef(expectedNumContainers));
    YarnService yarnService = new YarnService(config, "testApplicationName", "testApplicationId", yarnConfiguration, mockFileSystem, eventBus);
    YarnService yarnServiceSpy = Mockito.spy(yarnService);
    Mockito.doNothing().when(yarnServiceSpy).requestContainers(Mockito.anyInt(), Mockito.any(Resource.class), Mockito.any(Optional.class));
    yarnServiceSpy.startUp();
    Mockito.verify(yarnServiceSpy, Mockito.times(1)).requestContainers(Mockito.eq(expectedNumContainers), Mockito.any(Resource.class), Mockito.any(Optional.class));
  }

  @Test
  public void testBuildContainerCommand() throws Exception {
    final double jvmMemoryXmxRatio = 0.7;
    final int jvmMemoryOverheadMbs = 50;
    final int resourceMemoryMB = 3072;
    final int expectedJvmMemory = (int) (resourceMemoryMB * jvmMemoryXmxRatio) - jvmMemoryOverheadMbs;

    Config config = this.defaultConfigs
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_XMX_RATIO_KEY, ConfigValueFactory.fromAnyRef(jvmMemoryXmxRatio))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_JVM_MEMORY_OVERHEAD_MBS_KEY, ConfigValueFactory.fromAnyRef(jvmMemoryOverheadMbs));

    YarnService yarnService = new YarnService(
        config,
        "testApplicationName",
        "testApplicationId",
        yarnConfiguration,
        mockFileSystem,
        eventBus
    );

    WorkerProfile workerProfile = new WorkerProfile(config);
    ContainerId containerId = ContainerId.newContainerId(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 0),
        0), 0);
    Resource resource = Resource.newInstance(resourceMemoryMB, 2);
    Container container = Container.newInstance(containerId, null, null, resource, null, null);
    YarnService.ContainerInfo containerInfo = yarnService.new ContainerInfo(container, WorkforceProfiles.BASELINE_NAME_RENDERING, workerProfile);
    String command = containerInfo.getStartupCommand();
    Assert.assertTrue(command.contains("-Xmx" + expectedJvmMemory + "M"));
  }
}
