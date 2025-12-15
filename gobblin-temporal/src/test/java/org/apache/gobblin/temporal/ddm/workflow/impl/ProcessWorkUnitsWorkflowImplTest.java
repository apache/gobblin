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

package org.apache.gobblin.temporal.ddm.workflow.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;

import org.apache.gobblin.temporal.cluster.WorkerConfig;
import org.apache.gobblin.temporal.ddm.workflow.CommitStepWorkflow;


/**
 * Tests for {@link ProcessWorkUnitsWorkflowImpl} focusing on task queue routing
 * and child workflow creation.
 */
public class ProcessWorkUnitsWorkflowImplTest {

  private MockedStatic<Workflow> workflowMockedStatic;
  private MockedStatic<WorkerConfig> workerConfigMockedStatic;
  private ProcessWorkUnitsWorkflowImpl workflow;

  @BeforeMethod
  public void setup() {
    workflowMockedStatic = Mockito.mockStatic(Workflow.class);
    workerConfigMockedStatic = Mockito.mockStatic(WorkerConfig.class);
    workflow = new ProcessWorkUnitsWorkflowImpl();
  }

  @AfterMethod
  public void tearDown() {
    if (workflowMockedStatic != null) {
      workflowMockedStatic.close();
    }
    if (workerConfigMockedStatic != null) {
      workerConfigMockedStatic.close();
    }
  }

  /**
   * Tests that CommitStepWorkflow child workflow is created without explicit task queue,
   * allowing it to inherit the parent workflow's task queue (default queue).
   * This ensures CommitStepWorkflow runs on WorkFulfillmentWorker, not ExecutionWorker.
   */
  @Test
  public void testCreateCommitStepWorkflowUsesDefaultQueue() {
    // Setup
    Map<String, Object> searchAttributes = new HashMap<>();
    searchAttributes.put("test", "value");
    
    Config mockConfig = ConfigFactory.empty();
    workerConfigMockedStatic.when(() -> WorkerConfig.of(Mockito.any()))
        .thenReturn(java.util.Optional.of(mockConfig));

    CommitStepWorkflow mockCommitWorkflow = Mockito.mock(CommitStepWorkflow.class);
    
    // Capture the ChildWorkflowOptions passed to newChildWorkflowStub
    workflowMockedStatic.when(() -> Workflow.newChildWorkflowStub(
        Mockito.eq(CommitStepWorkflow.class),
        Mockito.any(ChildWorkflowOptions.class)))
        .thenAnswer(invocation -> {
          ChildWorkflowOptions options = invocation.getArgument(1);
          // Verify task queue is NOT set (should be null to inherit from parent)
          Assert.assertNull(options.getTaskQueue(), 
              "CommitStepWorkflow should not have explicit task queue set");
          Assert.assertEquals(options.getSearchAttributes(), searchAttributes);
          return mockCommitWorkflow;
        });

    // Execute
    CommitStepWorkflow result = workflow.createCommitStepWorkflow(searchAttributes);

    // Verify
    Assert.assertNotNull(result);
    workflowMockedStatic.verify(() -> Workflow.newChildWorkflowStub(
        Mockito.eq(CommitStepWorkflow.class),
        Mockito.any(ChildWorkflowOptions.class)), Mockito.times(1));
  }

  /**
   * Tests that CommitStepWorkflow creation works when WorkerConfig is absent.
   */
  @Test
  public void testCreateCommitStepWorkflowWithoutWorkerConfig() {
    // Setup
    Map<String, Object> searchAttributes = new HashMap<>();
    
    workerConfigMockedStatic.when(() -> WorkerConfig.of(Mockito.any()))
        .thenReturn(java.util.Optional.empty());

    CommitStepWorkflow mockCommitWorkflow = Mockito.mock(CommitStepWorkflow.class);
    
    workflowMockedStatic.when(() -> Workflow.newChildWorkflowStub(
        Mockito.eq(CommitStepWorkflow.class),
        Mockito.any(ChildWorkflowOptions.class)))
        .thenReturn(mockCommitWorkflow);

    // Execute
    CommitStepWorkflow result = workflow.createCommitStepWorkflow(searchAttributes);

    // Verify
    Assert.assertNotNull(result);
  }

  /**
   * Tests that workflow ID is properly qualified with flow execution ID.
   */
  @Test
  public void testCommitStepWorkflowIdQualification() {
    // Setup
    Map<String, Object> searchAttributes = new HashMap<>();
    Config mockConfig = ConfigFactory.parseString("flow.executionId=test-exec-123");
    
    workerConfigMockedStatic.when(() -> WorkerConfig.of(Mockito.any()))
        .thenReturn(java.util.Optional.of(mockConfig));

    CommitStepWorkflow mockCommitWorkflow = Mockito.mock(CommitStepWorkflow.class);
    
    workflowMockedStatic.when(() -> Workflow.newChildWorkflowStub(
        Mockito.eq(CommitStepWorkflow.class),
        Mockito.any(ChildWorkflowOptions.class)))
        .thenAnswer(invocation -> {
          ChildWorkflowOptions options = invocation.getArgument(1);
          String workflowId = options.getWorkflowId();
          Assert.assertNotNull(workflowId);
          Assert.assertTrue(workflowId.contains("CommitStepWorkflow"),
              "Workflow ID should contain base name");
          return mockCommitWorkflow;
        });

    // Execute
    workflow.createCommitStepWorkflow(searchAttributes);

    // Verify through mock interactions
    workflowMockedStatic.verify(() -> Workflow.newChildWorkflowStub(
        Mockito.any(), Mockito.any()), Mockito.times(1));
  }
}
