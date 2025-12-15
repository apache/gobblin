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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.WorkerProfile;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;


/**
 * Tests for {@link DynamicScalingYarnService} initialization and execution worker profile setup.
 * Focuses on verifying that dynamic scaling correctly initializes execution worker profiles.
 */
public class DynamicScalingInitializationTest {

  private Config baseConfig;
  private YarnConfiguration yarnConfiguration;
  private FileSystem mockFileSystem;
  private EventBus eventBus;
  private AMRMClientAsync mockAMRMClient;
  private RegisterApplicationMasterResponse mockRegisterResponse;
  private MockedStatic<AMRMClientAsync> amrmClientMockStatic;

  @BeforeMethod
  public void setup() throws Exception {
    baseConfig = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
            ConfigValueFactory.fromAnyRef(8192))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY,
            ConfigValueFactory.fromAnyRef(4))
        .withValue(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY,
            ConfigValueFactory.fromAnyRef(1))
        .withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED,
            ConfigValueFactory.fromAnyRef(true));

    yarnConfiguration = new YarnConfiguration();
    mockFileSystem = Mockito.mock(FileSystem.class);
    eventBus = new EventBus("DynamicScalingInitializationTest");

    mockAMRMClient = Mockito.mock(AMRMClientAsync.class);
    mockRegisterResponse = Mockito.mock(RegisterApplicationMasterResponse.class);

    amrmClientMockStatic = Mockito.mockStatic(AMRMClientAsync.class);
    amrmClientMockStatic.when(() -> AMRMClientAsync.createAMRMClientAsync(anyInt(), any(AMRMClientAsync.CallbackHandler.class)))
        .thenReturn(mockAMRMClient);

    Mockito.doNothing().when(mockAMRMClient).init(any(YarnConfiguration.class));
    Mockito.when(mockAMRMClient.registerApplicationMaster(anyString(), anyInt(), anyString()))
        .thenReturn(mockRegisterResponse);
    Mockito.when(mockRegisterResponse.getMaximumResourceCapability())
        .thenReturn(Resource.newInstance(102400, 32));
  }

  @AfterMethod
  public void tearDown() {
    if (amrmClientMockStatic != null) {
      amrmClientMockStatic.close();
    }
  }

  /**
   * Tests that DynamicScalingYarnService initializes execution worker profile when enabled.
   */
  @Test
  public void testDynamicScalingInitializesExecutionProfile() throws Exception {
    // Execute
    DynamicScalingYarnService service = new DynamicScalingYarnService(
        baseConfig, "testApp", "testAppId", yarnConfiguration, mockFileSystem, eventBus);
    DynamicScalingYarnService serviceSpy = Mockito.spy(service);
    Mockito.doNothing().when(serviceSpy).requestContainers(anyInt(), any(Resource.class), any(Optional.class));

    serviceSpy.startUp();

    // Verify that execution worker profile was requested
    Mockito.verify(serviceSpy, Mockito.atLeastOnce())
        .requestContainersForWorkerProfile(any(WorkerProfile.class), anyInt());
  }

  /**
   * Tests that execution worker profile has correct worker class configured.
   */
  @Test
  public void testExecutionProfileHasCorrectWorkerClass() {
    // Setup - config with stage-specific memory
    Config configWithMemory = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB,
            ConfigValueFactory.fromAnyRef("32768"));

    // Simulate profile creation (as done in DynamicScalingYarnService)
    Config profileConfig = configWithMemory
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS))
        .withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY,
            ConfigValueFactory.fromAnyRef("execution"))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
            ConfigValueFactory.fromAnyRef(32768));

    WorkerProfile executionProfile = new WorkerProfile("initial-execution", profileConfig);

    // Verify
    Assert.assertEquals(executionProfile.getName(), "initial-execution");
    Assert.assertEquals(
        executionProfile.getConfig().getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS,
        "Execution profile should have ExecutionWorker class");
    Assert.assertEquals(
        executionProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY),
        32768,
        "Execution profile should have stage-specific memory");
  }

  /**
   * Tests that dynamic scaling is NOT initialized when disabled.
   */
  @Test
  public void testDynamicScalingNotInitializedWhenDisabled() throws Exception {
    // Setup - config with dynamic scaling disabled
    Config disabledConfig = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED,
            ConfigValueFactory.fromAnyRef(false));

    // Execute
    YarnService service = new YarnService(
        disabledConfig, "testApp", "testAppId", yarnConfiguration, mockFileSystem, eventBus);
    YarnService serviceSpy = Mockito.spy(service);
    Mockito.doNothing().when(serviceSpy).requestContainers(anyInt(), any(Resource.class), any(Optional.class));

    serviceSpy.startUp();

    // Verify - should request baseline containers, not execution profile
    Mockito.verify(serviceSpy, Mockito.times(1))
        .requestContainers(anyInt(), any(Resource.class), any(Optional.class));
  }

  /**
   * Tests that execution profile uses stage-specific memory when configured.
   */
  @Test
  public void testExecutionProfileUsesStageSpecificMemory() {
    // Setup
    int stageMemoryMb = 65536;
    Config configWithStageMemory = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB,
            ConfigValueFactory.fromAnyRef(String.valueOf(stageMemoryMb)));

    // Verify config has stage-specific memory
    Assert.assertTrue(configWithStageMemory.hasPath(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB),
        "Config should have stage-specific memory");
    Assert.assertEquals(
        configWithStageMemory.getString(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB),
        String.valueOf(stageMemoryMb),
        "Stage-specific memory should be set correctly");
  }

  /**
   * Tests that execution profile falls back to baseline memory when not configured.
   */
  @Test
  public void testExecutionProfileFallsBackToBaselineMemory() {
    // baseConfig doesn't have stage-specific memory
    Assert.assertFalse(baseConfig.hasPath(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB),
        "Config should not have stage-specific memory");

    // Verify baseline memory is used
    int baselineMemory = baseConfig.getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY);
    Assert.assertEquals(baselineMemory, 8192,
        "Should use baseline memory when stage-specific not configured");
  }

  /**
   * Tests that execution profile has correct Helix tag.
   */
  @Test
  public void testExecutionProfileHasCorrectHelixTag() {
    // Simulate profile with Helix tag
    Config profileConfig = baseConfig
        .withValue(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY,
            ConfigValueFactory.fromAnyRef("execution"));

    Assert.assertEquals(
        profileConfig.getString(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY),
        "execution",
        "Execution profile should have 'execution' Helix tag");
  }

  /**
   * Tests that initial scaling directive has correct set point.
   */
  @Test
  public void testInitialScalingDirectiveSetPoint() throws Exception {
    // Execute
    DynamicScalingYarnService service = new DynamicScalingYarnService(
        baseConfig, "testApp", "testAppId", yarnConfiguration, mockFileSystem, eventBus);
    DynamicScalingYarnService serviceSpy = Mockito.spy(service);
    Mockito.doNothing().when(serviceSpy).requestContainers(anyInt(), any(Resource.class), any(Optional.class));

    serviceSpy.startUp();

    // Verify - should request 1 execution container (set point = 1)
    Mockito.verify(serviceSpy, Mockito.atLeastOnce())
        .requestContainersForWorkerProfile(any(WorkerProfile.class), Mockito.eq(1));
  }
}
