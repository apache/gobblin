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

import java.util.Collections;

import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.WorkerProfile;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;


/** Tests for {@link DynamicScalingYarnService} */
public class DynamicScalingYarnServiceTest {
  private Config defaultConfigs;
  private final int initNumContainers = 1;
  private final int initMemoryMbs = 1024;
  private final int initCores = 1;
  private final Resource initResource = Resource.newInstance(initMemoryMbs, initCores);
  private final YarnConfiguration yarnConfiguration = new YarnConfiguration();
  private final FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
  private final EventBus eventBus = new EventBus("TemporalDynamicScalingYarnServiceTest");
  private AMRMClientAsync mockAMRMClient;
  private RegisterApplicationMasterResponse mockRegisterApplicationMasterResponse;
  private WorkerProfile testBaselineworkerProfile;
  private DynamicScalingYarnService dynamicScalingYarnServiceSpy;

  @BeforeClass
  public void setup() throws Exception {
    this.defaultConfigs = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY, ConfigValueFactory.fromAnyRef(initCores))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY, ConfigValueFactory.fromAnyRef(initMemoryMbs))
        .withValue(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY, ConfigValueFactory.fromAnyRef(initNumContainers));

    this.testBaselineworkerProfile = new WorkerProfile(this.defaultConfigs);

    mockAMRMClient = Mockito.mock(AMRMClientAsync.class);
    mockRegisterApplicationMasterResponse = Mockito.mock(RegisterApplicationMasterResponse.class);

    MockedStatic<AMRMClientAsync> amrmClientAsyncMockStatic = Mockito.mockStatic(AMRMClientAsync.class);

    amrmClientAsyncMockStatic.when(() -> AMRMClientAsync.createAMRMClientAsync(anyInt(), any(AMRMClientAsync.CallbackHandler.class)))
        .thenReturn(mockAMRMClient);
    Mockito.doNothing().when(mockAMRMClient).init(any(YarnConfiguration.class));

    Mockito.when(mockAMRMClient.registerApplicationMaster(anyString(), anyInt(), anyString()))
        .thenReturn(mockRegisterApplicationMasterResponse);
    Mockito.when(mockRegisterApplicationMasterResponse.getMaximumResourceCapability())
        .thenReturn(Mockito.mock(Resource.class));
  }

  @BeforeMethod
  public void setupMethod() throws Exception {
    DynamicScalingYarnService dynamicScalingYarnService = new DynamicScalingYarnService(this.defaultConfigs, "testApp", "testAppId", yarnConfiguration, mockFileSystem, eventBus);
    dynamicScalingYarnServiceSpy = Mockito.spy(dynamicScalingYarnService);
    Mockito.doNothing().when(dynamicScalingYarnServiceSpy).requestContainers(Mockito.anyInt(), Mockito.any(Resource.class), Mockito.any(Optional.class));
    dynamicScalingYarnServiceSpy.containerMap.clear();
  }

  @AfterMethod
  public void cleanupMethod() {
    dynamicScalingYarnServiceSpy.containerMap.clear();
    Mockito.reset(dynamicScalingYarnServiceSpy);
  }

  @Test
  public void testDynamicScalingYarnServiceStartupWithInitialContainers() throws Exception {
    dynamicScalingYarnServiceSpy.startUp();
    ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(0)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(1)).requestContainersForWorkerProfile(Mockito.any(WorkerProfile.class), Mockito.anyInt());
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(1)).requestContainers(Mockito.eq(initNumContainers), resourceCaptor.capture(), Mockito.any(Optional.class));
    Resource capturedResource = resourceCaptor.getValue();
    Assert.assertEquals(capturedResource.getMemorySize(), initMemoryMbs);
    Assert.assertEquals(capturedResource.getVirtualCores(), initCores);
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

  @DataProvider(name = "OOMExitStatusProvider")
  public Object[][] OOMExitStatusProvider() {
    return new Object[][] {
        {ContainerExitStatus.KILLED_EXCEEDED_PMEM},
        {ContainerExitStatus.KILLED_EXCEEDED_VMEM},
        {DynamicScalingYarnService.GENERAL_OOM_EXIT_STATUS_CODE}
    };
  }

  @DataProvider(name = "NonOOMExitStatusProviderWhichRequestReplacementContainer")
  public Object[][] NonOOMExitStatusProviderWhichRequestReplacementContainer() {
    return new Object[][] {
        {ContainerExitStatus.ABORTED},
        {ContainerExitStatus.PREEMPTED}
    };
  }

  @Test(dataProvider = "OOMExitStatusProvider")
  public void testHandleContainerCompletionForStatusOOM(int containerExitStatusCode) throws Exception {
    ContainerId containerId = generateRandomContainerId();
    DynamicScalingYarnService.ContainerInfo containerInfo = createBaselineContainerInfo(containerId);
    ContainerStatus containerStatus = Mockito.mock(ContainerStatus.class);
    Mockito.when(containerStatus.getContainerId()).thenReturn(containerId);
    Mockito.when(containerStatus.getExitStatus()).thenReturn(containerExitStatusCode);

    dynamicScalingYarnServiceSpy.startUp();
    dynamicScalingYarnServiceSpy.containerMap.put(containerId, containerInfo); // Required to be done for test otherwise containerMap is always empty since it is updated after containers are allocated

    dynamicScalingYarnServiceSpy.handleContainerCompletion(containerStatus);

    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(1)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(2)).requestContainersForWorkerProfile(Mockito.any(WorkerProfile.class), Mockito.anyInt());
    ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(2)).requestContainers(Mockito.eq(1), resourceCaptor.capture(), Mockito.any(Optional.class));

    Resource capturedResource = resourceCaptor.getValue();
    Assert.assertEquals(capturedResource.getMemorySize(), (long) initMemoryMbs * DynamicScalingYarnService.DEFAULT_REPLACEMENT_CONTAINER_MEMORY_MULTIPLIER);
    Assert.assertEquals(capturedResource.getVirtualCores(), initCores);
  }

  @Test(dataProvider = "NonOOMExitStatusProviderWhichRequestReplacementContainer")
  public void testHandleContainerCompletionForNonOOMStatusWhichRequestReplacementContainer(int containerExitStatusCode) throws Exception {
    ContainerId containerId = generateRandomContainerId();
    DynamicScalingYarnService.ContainerInfo containerInfo = createBaselineContainerInfo(containerId);
    ContainerStatus containerStatus = Mockito.mock(ContainerStatus.class);
    Mockito.when(containerStatus.getContainerId()).thenReturn(containerId);
    Mockito.when(containerStatus.getExitStatus()).thenReturn(containerExitStatusCode);

    dynamicScalingYarnServiceSpy.startUp();
    dynamicScalingYarnServiceSpy.containerMap.put(containerId, containerInfo); // Required to be done for test otherwise containerMap is always empty since it is updated after containers are allocated

    dynamicScalingYarnServiceSpy.handleContainerCompletion(containerStatus);
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(0)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(2)).requestContainersForWorkerProfile(Mockito.any(WorkerProfile.class), Mockito.anyInt());
    ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(2)).requestContainers(Mockito.eq(1), resourceCaptor.capture(), Mockito.any(Optional.class));

    Resource capturedResource = resourceCaptor.getValue();
    Assert.assertEquals(capturedResource.getMemorySize(), initMemoryMbs);
    Assert.assertEquals(capturedResource.getVirtualCores(), initCores);
  }

  @Test
  public void testHandleContainerCompletionForAllOOMStatus() throws Exception {
    ContainerId containerId1 = generateRandomContainerId();
    ContainerId containerId2 = generateRandomContainerId();
    ContainerId containerId3 = generateRandomContainerId();

    DynamicScalingYarnService.ContainerInfo containerInfo1 = createBaselineContainerInfo(containerId1);
    DynamicScalingYarnService.ContainerInfo containerInfo2 = createBaselineContainerInfo(containerId2);
    DynamicScalingYarnService.ContainerInfo containerInfo3 = createBaselineContainerInfo(containerId3);

    ContainerStatus containerStatus1 = Mockito.mock(ContainerStatus.class);
    Mockito.when(containerStatus1.getContainerId()).thenReturn(containerId1);
    Mockito.when(containerStatus1.getExitStatus()).thenReturn(ContainerExitStatus.KILLED_EXCEEDED_VMEM);

    ContainerStatus containerStatus2 = Mockito.mock(ContainerStatus.class);
    Mockito.when(containerStatus2.getContainerId()).thenReturn(containerId2);
    Mockito.when(containerStatus2.getExitStatus()).thenReturn(DynamicScalingYarnService.GENERAL_OOM_EXIT_STATUS_CODE);

    ContainerStatus containerStatus3 = Mockito.mock(ContainerStatus.class);
    Mockito.when(containerStatus3.getContainerId()).thenReturn(containerId3);
    Mockito.when(containerStatus3.getExitStatus()).thenReturn(ContainerExitStatus.KILLED_EXCEEDED_PMEM);

    dynamicScalingYarnServiceSpy.startUp();
    // Required to be done for test otherwise containerMap is always empty since it is updated after containers are allocated
    dynamicScalingYarnServiceSpy.containerMap.put(containerId1, containerInfo1);
    dynamicScalingYarnServiceSpy.containerMap.put(containerId2, containerInfo2);
    dynamicScalingYarnServiceSpy.containerMap.put(containerId3, containerInfo3);

    dynamicScalingYarnServiceSpy.handleContainerCompletion(containerStatus1);
    dynamicScalingYarnServiceSpy.handleContainerCompletion(containerStatus2);
    dynamicScalingYarnServiceSpy.handleContainerCompletion(containerStatus3);

    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(3)).reviseWorkforcePlanAndRequestNewContainers(Mockito.anyList());
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(4)).requestContainersForWorkerProfile(Mockito.any(WorkerProfile.class), Mockito.anyInt());
    ArgumentCaptor<Resource> resourceCaptor = ArgumentCaptor.forClass(Resource.class);
    Mockito.verify(dynamicScalingYarnServiceSpy, Mockito.times(4)).requestContainers(Mockito.eq(1), resourceCaptor.capture(), Mockito.any(Optional.class));

    List<Resource> capturedResources = resourceCaptor.getAllValues();
    Assert.assertEquals(capturedResources.size(), 4);

    Resource capturedResource = capturedResources.get(0);
    Assert.assertEquals(capturedResource.getMemorySize(), initMemoryMbs);
    Assert.assertEquals(capturedResource.getVirtualCores(), initCores);

    for (int idx = 1 ; idx < 4 ; idx++) {
      capturedResource = capturedResources.get(idx);
      Assert.assertEquals(capturedResource.getMemorySize(), (long) initMemoryMbs * DynamicScalingYarnService.DEFAULT_REPLACEMENT_CONTAINER_MEMORY_MULTIPLIER);
      Assert.assertEquals(capturedResource.getVirtualCores(), initCores);
    }
  }

  private ContainerId generateRandomContainerId() {
    return ContainerId.newContainerId(ApplicationAttemptId.newInstance(ApplicationId.newInstance(1, 0),
        0), (long) (Math.random() * 1000));
  }

  private DynamicScalingYarnService.ContainerInfo createBaselineContainerInfo(ContainerId containerId) {
    Container container = Container.newInstance(containerId, null, null, initResource, null, null);
    return dynamicScalingYarnServiceSpy.new ContainerInfo(container, WorkforceProfiles.BASELINE_NAME_RENDERING, testBaselineworkerProfile);
  }
}
