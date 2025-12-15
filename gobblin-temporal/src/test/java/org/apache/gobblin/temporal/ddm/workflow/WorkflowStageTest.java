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

package org.apache.gobblin.temporal.ddm.workflow;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;


/**
 * Tests for {@link WorkflowStage} to verify task queue configuration
 * for different workflow stages.
 */
public class WorkflowStageTest {

  /**
   * Tests that WORK_EXECUTION stage uses execution task queue from config.
   */
  @Test
  public void testWorkExecutionStageUsesExecutionQueue() {
    // Setup
    String customExecutionQueue = "CustomExecutionQueue";
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef(customExecutionQueue));

    // Execute
    String taskQueue = WorkflowStage.WORK_EXECUTION.getTaskQueue(config);

    // Verify
    Assert.assertEquals(taskQueue, customExecutionQueue,
        "WORK_EXECUTION should use configured execution queue");
  }

  /**
   * Tests that WORK_EXECUTION stage falls back to default execution queue.
   */
  @Test
  public void testWorkExecutionStageUsesDefaultQueue() {
    // Setup - empty config
    Config config = ConfigFactory.empty();

    // Execute
    String taskQueue = WorkflowStage.WORK_EXECUTION.getTaskQueue(config);

    // Verify
    Assert.assertEquals(taskQueue, GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE,
        "WORK_EXECUTION should use default execution queue when not configured");
  }

  /**
   * Tests that WORK_DISCOVERY stage uses default task queue.
   */
  @Test
  public void testWorkDiscoveryStageUsesDefaultQueue() {
    // Setup
    Config config = ConfigFactory.empty();

    // Execute
    String taskQueue = WorkflowStage.WORK_DISCOVERY.getTaskQueue(config);

    // Verify
    Assert.assertEquals(taskQueue, GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE,
        "WORK_DISCOVERY should use default task queue");
  }

  /**
   * Tests that WORK_COMMIT stage uses default task queue.
   */
  @Test
  public void testWorkCommitStageUsesDefaultQueue() {
    // Setup
    Config config = ConfigFactory.empty();

    // Execute
    String taskQueue = WorkflowStage.COMMIT.getTaskQueue(config);

    // Verify
    Assert.assertEquals(taskQueue, GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE,
        "WORK_COMMIT should use default task queue");
  }

  /**
   * Tests that different stages use different task queues in dynamic scaling mode.
   */
  @Test
  public void testDifferentStagesUseDifferentQueues() {
    // Setup
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef("ExecutionQueue"))
        .withValue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef("DefaultQueue"));

    // Execute
    String executionQueue = WorkflowStage.WORK_EXECUTION.getTaskQueue(config);
    String discoveryQueue = WorkflowStage.WORK_DISCOVERY.getTaskQueue(config);
    String commitQueue = WorkflowStage.COMMIT.getTaskQueue(config);

    // Verify
    Assert.assertEquals(executionQueue, "ExecutionQueue");
    Assert.assertEquals(discoveryQueue, "DefaultQueue");
    Assert.assertEquals(commitQueue, "DefaultQueue");
    Assert.assertNotEquals(executionQueue, discoveryQueue,
        "Execution and discovery should use different queues");
  }

  /**
   * Tests that execution queue configuration is independent of default queue.
   */
  @Test
  public void testExecutionQueueIndependentOfDefaultQueue() {
    // Setup - only configure execution queue
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef("CustomExecutionQueue"));

    // Execute
    String executionQueue = WorkflowStage.WORK_EXECUTION.getTaskQueue(config);
    String discoveryQueue = WorkflowStage.WORK_DISCOVERY.getTaskQueue(config);

    // Verify
    Assert.assertEquals(executionQueue, "CustomExecutionQueue",
        "Should use custom execution queue");
    Assert.assertEquals(discoveryQueue, GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE,
        "Should use default queue for discovery");
  }

  /**
   * Tests that default execution queue constant is correct.
   */
  @Test
  public void testDefaultExecutionQueueConstant() {
    Assert.assertEquals(GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE,
        "GobblinTemporalExecutionQueue",
        "Default execution queue should be GobblinTemporalExecutionQueue");
  }

  /**
   * Tests that default task queue constant is correct.
   */
  @Test
  public void testDefaultTaskQueueConstant() {
    Assert.assertEquals(GobblinTemporalConfigurationKeys.DEFAULT_GOBBLIN_TEMPORAL_TASK_QUEUE,
        "GobblinTemporalTaskQueue",
        "Default task queue should be GobblinTemporalTaskQueue");
  }

  /**
   * Tests task queue configuration for all workflow stages.
   */
  @Test
  public void testAllWorkflowStagesHaveTaskQueues() {
    Config config = ConfigFactory.empty();

    for (WorkflowStage stage : WorkflowStage.values()) {
      String taskQueue = stage.getTaskQueue(config);
      Assert.assertNotNull(taskQueue, "Stage " + stage + " should have a task queue");
      Assert.assertFalse(taskQueue.isEmpty(), "Stage " + stage + " task queue should not be empty");
    }
  }

  /**
   * Tests that execution queue config key is correct.
   */
  @Test
  public void testExecutionQueueConfigKey() {
    Assert.assertEquals(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
        "gobblin.temporal.execution.task.queue.name",
        "Execution task queue config key should match expected value");
  }

  /**
   * Tests that task queue config key is correct.
   */
  @Test
  public void testTaskQueueConfigKey() {
    Assert.assertEquals(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
        "gobblin.temporal.task.queue.name",
        "Task queue config key should match expected value");
  }
}
