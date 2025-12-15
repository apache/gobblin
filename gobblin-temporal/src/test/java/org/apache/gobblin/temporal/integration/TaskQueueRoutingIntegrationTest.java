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

package org.apache.gobblin.temporal.integration;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.worker.ExecutionWorker;
import org.apache.gobblin.temporal.ddm.workflow.WorkflowStage;
import org.apache.gobblin.temporal.dynamic.ProfileOverlay;
import org.apache.gobblin.temporal.dynamic.WorkerProfile;
import org.apache.gobblin.temporal.workflows.helloworld.HelloWorldWorker;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;

import java.util.Arrays;
import java.util.List;


/**
 * Integration tests verifying end-to-end task queue routing for dynamic scaling.
 * Tests the complete flow from profile creation to worker configuration.
 */
public class TaskQueueRoutingIntegrationTest {

  private Config baselineConfig;

  @BeforeMethod
  public void setup() {
    baselineConfig = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
            ConfigValueFactory.fromAnyRef(8192))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY,
            ConfigValueFactory.fromAnyRef(4))
        .withValue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef("GobblinTemporalTaskQueue"))
        .withValue(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef("GobblinTemporalExecutionQueue"))
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(HelloWorldWorker.class.getName()));
  }

  /**
   * Integration test: Baseline container should use default queue and WorkFulfillmentWorker.
   */
  @Test
  public void testBaselineContainerConfiguration() {
    // Baseline container config
    WorkerProfile baselineProfile = new WorkerProfile("baseline", baselineConfig);

    // Verify worker class
    Assert.assertEquals(
        baselineProfile.getConfig().getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        HelloWorldWorker.class.getName(),
        "Baseline should use HelloWorldWorker (default)");

    // Verify task queue (would be used by AbstractTemporalWorker.getTaskQueue())
    String taskQueue = baselineProfile.getConfig().getString(
        GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE);
    Assert.assertEquals(taskQueue, "GobblinTemporalTaskQueue",
        "Baseline should use default task queue");
  }

  /**
   * Integration test: Execution container should use execution queue and ExecutionWorker.
   */
  @Test
  public void testExecutionContainerConfiguration() {
    // Create execution profile overlay
    List<ProfileOverlay.KVPair> overlayPairs = Arrays.asList(
        new ProfileOverlay.KVPair(
            GobblinTemporalConfigurationKeys.WORKER_CLASS,
            GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS
        ),
        new ProfileOverlay.KVPair(
            GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY,
            "execution"
        )
    );

    ProfileOverlay executionOverlay = new ProfileOverlay.Adding(overlayPairs);
    Config executionConfig = executionOverlay.applyOverlay(baselineConfig);
    WorkerProfile executionProfile = new WorkerProfile("execution", executionConfig);

    // Verify worker class
    Assert.assertEquals(
        executionProfile.getConfig().getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        ExecutionWorker.class.getName(),
        "Execution profile should use ExecutionWorker");

    // Verify ExecutionWorker would use execution queue
    String executionQueue = WorkflowStage.WORK_EXECUTION.getTaskQueue(executionConfig);
    Assert.assertEquals(executionQueue, "GobblinTemporalExecutionQueue",
        "Execution worker should use execution task queue");
  }

  /**
   * Integration test: ProcessWorkUnitsWorkflow routes to execution queue when dynamic scaling enabled.
   */
  @Test
  public void testProcessWorkUnitsWorkflowRoutingWithDynamicScaling() {
    // Setup - config with dynamic scaling enabled
    Config configWithScaling = baselineConfig
        .withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED,
            ConfigValueFactory.fromAnyRef(true));

    // Verify execution queue is configured
    String executionQueue = WorkflowStage.WORK_EXECUTION.getTaskQueue(configWithScaling);
    Assert.assertEquals(executionQueue, "GobblinTemporalExecutionQueue",
        "ProcessWorkUnitsWorkflow should route to execution queue");
  }

  /**
   * Integration test: CommitStepWorkflow always uses default queue (no explicit routing).
   */
  @Test
  public void testCommitStepWorkflowRoutingToDefaultQueue() {
    // CommitStepWorkflow should use default queue regardless of dynamic scaling
    String commitQueue = WorkflowStage.COMMIT.getTaskQueue(baselineConfig);
    Assert.assertEquals(commitQueue, "GobblinTemporalTaskQueue",
        "CommitStepWorkflow should use default task queue");

    // Even with dynamic scaling enabled
    Config configWithScaling = baselineConfig
        .withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED,
            ConfigValueFactory.fromAnyRef(true));
    
    String commitQueueWithScaling = WorkflowStage.COMMIT.getTaskQueue(configWithScaling);
    Assert.assertEquals(commitQueueWithScaling, "GobblinTemporalTaskQueue",
        "CommitStepWorkflow should still use default queue with dynamic scaling");
  }

  /**
   * Integration test: Complete flow from baseline to execution profile.
   */
  @Test
  public void testCompleteProfileDerivationFlow() {
    // Step 1: Start with baseline profile
    WorkerProfile baselineProfile = new WorkerProfile("baseline", baselineConfig);
    Assert.assertEquals(
        baselineProfile.getConfig().getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        HelloWorldWorker.class.getName());

    // Step 2: Create execution overlay
    List<ProfileOverlay.KVPair> overlayPairs = Arrays.asList(
        new ProfileOverlay.KVPair(
            GobblinTemporalConfigurationKeys.WORKER_CLASS,
            GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS
        ),
        new ProfileOverlay.KVPair(
            GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
            "65536"
        )
    );
    ProfileOverlay executionOverlay = new ProfileOverlay.Adding(overlayPairs);

    // Step 3: Apply overlay to create execution profile
    Config executionConfig = executionOverlay.applyOverlay(baselineConfig);
    WorkerProfile executionProfile = new WorkerProfile("execution-derived", executionConfig);

    // Step 4: Verify execution profile has correct settings
    Assert.assertEquals(
        executionProfile.getConfig().getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        ExecutionWorker.class.getName(),
        "Derived profile should have ExecutionWorker");
    Assert.assertEquals(
        executionProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY),
        65536,
        "Derived profile should have increased memory");
    Assert.assertEquals(
        executionProfile.getConfig().getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY),
        4,
        "Derived profile should preserve baseline cores");
  }

  /**
   * Integration test: Verify worker class constants are correctly defined.
   */
  @Test
  public void testWorkerClassConstants() {
    // Verify constants match actual class names
    Assert.assertEquals(
        GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS,
        ExecutionWorker.class.getName(),
        "EXECUTION_WORKER_CLASS constant should match ExecutionWorker class name");

    Assert.assertEquals(
        GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS,
        HelloWorldWorker.class.getName(),
        "DEFAULT_WORKER_CLASS constant should match HelloWorldWorker class name");
  }

  /**
   * Integration test: Verify task queue constants are correctly defined.
   */
  @Test
  public void testTaskQueueConstants() {
    Assert.assertEquals(
        GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE,
        "GobblinTemporalExecutionQueue",
        "Default execution queue constant should be correct");

    Assert.assertEquals(
        GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE,
        "GobblinTemporalTaskQueue",
        "Default task queue constant should be correct");
  }

  /**
   * Integration test: Verify different workflow stages route to correct queues.
   */
  @Test
  public void testWorkflowStageRouting() {
    Config config = baselineConfig
        .withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED,
            ConfigValueFactory.fromAnyRef(true));

    // Discovery activities -> default queue
    String discoveryQueue = WorkflowStage.WORK_DISCOVERY.getTaskQueue(config);
    Assert.assertEquals(discoveryQueue, "GobblinTemporalTaskQueue");

    // Execution activities -> execution queue
    String executionQueue = WorkflowStage.WORK_EXECUTION.getTaskQueue(config);
    Assert.assertEquals(executionQueue, "GobblinTemporalExecutionQueue");

    // Commit activities -> default queue
    String commitQueue = WorkflowStage.COMMIT.getTaskQueue(config);
    Assert.assertEquals(commitQueue, "GobblinTemporalTaskQueue");

    // Verify execution uses different queue
    Assert.assertNotEquals(executionQueue, discoveryQueue,
        "Execution should use different queue than discovery");
    Assert.assertNotEquals(executionQueue, commitQueue,
        "Execution should use different queue than commit");
  }

  /**
   * Integration test: Verify memory override in execution profile.
   */
  @Test
  public void testExecutionProfileMemoryOverride() {
    // Setup - config with stage-specific memory
    Config configWithStageMemory = baselineConfig
        .withValue(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB,
            ConfigValueFactory.fromAnyRef("32768"));

    // Create execution profile with memory override
    ProfileOverlay.KVPair memoryOverride = new ProfileOverlay.KVPair(
        GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
        configWithStageMemory.getString(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB)
    );

    ProfileOverlay overlay = new ProfileOverlay.Adding(memoryOverride);
    Config executionConfig = overlay.applyOverlay(baselineConfig);

    // Verify memory was overridden
    Assert.assertEquals(
        executionConfig.getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY),
        32768,
        "Execution profile should have stage-specific memory");

    // Verify baseline memory is different
    Assert.assertEquals(
        baselineConfig.getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY),
        8192,
        "Baseline should have original memory");
  }
}
