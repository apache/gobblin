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

package org.apache.gobblin.temporal.ddm.worker;

import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import io.temporal.client.WorkflowClient;
import io.temporal.worker.WorkerFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;


/**
 * Tests for {@link ExecutionWorker} configuration logic.
 */
public class ExecutionWorkerTest {
  
  private MockedStatic<WorkerFactory> mockedWorkerFactory;
  private WorkerFactory mockFactory;

  @BeforeMethod
  public void setUp() {
    // Mock the static WorkerFactory.newInstance() method
    mockFactory = Mockito.mock(WorkerFactory.class);
    mockedWorkerFactory = Mockito.mockStatic(WorkerFactory.class);
    mockedWorkerFactory.when(() -> WorkerFactory.newInstance(Mockito.any(WorkflowClient.class)))
        .thenReturn(mockFactory);
  }

  @AfterMethod
  public void tearDown() {
    if (mockedWorkerFactory != null) {
      mockedWorkerFactory.close();
    }
  }

  /**
   * Tests that concurrency fields are initialized with custom values from config.
   */
  @Test
  public void testConcurrencyFieldsWithCustomConfig() {
    int customActivityConcurrency = 10;
    int customLocalActivityConcurrency = 8;
    int customWorkflowTaskConcurrency = 12;
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_EXECUTION_MAX_CONCURRENT_ACTIVITY_SIZE,
            ConfigValueFactory.fromAnyRef(customActivityConcurrency))
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_EXECUTION_MAX_CONCURRENT_LOCAL_ACTIVITY_SIZE,
            ConfigValueFactory.fromAnyRef(customLocalActivityConcurrency))
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_EXECUTION_MAX_CONCURRENT_WORKFLOW_TASK_SIZE,
            ConfigValueFactory.fromAnyRef(customWorkflowTaskConcurrency));
    
    WorkflowClient mockClient = Mockito.mock(WorkflowClient.class);
    ExecutionWorker worker = new ExecutionWorker(config, mockClient);
    
    Assert.assertEquals(worker.getMaxConcurrentActivityExecutionSize(), customActivityConcurrency);
    Assert.assertEquals(worker.getMaxConcurrentLocalActivityExecutionSize(), customLocalActivityConcurrency);
    Assert.assertEquals(worker.getMaxConcurrentWorkflowTaskExecutionSize(), customWorkflowTaskConcurrency);
  }

  /**
   * Tests that concurrency fields fall back to TEMPORAL_NUM_THREADS_PER_EXECUTION_WORKER.
   */
  @Test
  public void testConcurrencyFieldsFallbackToExecutionWorkerThreads() {
    int executionWorkerThreads = 20;
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_EXECUTION_WORKER,
            ConfigValueFactory.fromAnyRef(executionWorkerThreads));
    
    WorkflowClient mockClient = Mockito.mock(WorkflowClient.class);
    ExecutionWorker worker = new ExecutionWorker(config, mockClient);
    
    Assert.assertEquals(worker.getMaxConcurrentActivityExecutionSize(), executionWorkerThreads);
    Assert.assertEquals(worker.getMaxConcurrentLocalActivityExecutionSize(), executionWorkerThreads);
    Assert.assertEquals(worker.getMaxConcurrentWorkflowTaskExecutionSize(), executionWorkerThreads);
  }

  /**
   * Tests that concurrency fields fall back to TEMPORAL_NUM_THREADS_PER_WORKER.
   */
  @Test
  public void testConcurrencyFieldsFallbackToWorkerThreads() {
    int workerThreads = 25;
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            ConfigValueFactory.fromAnyRef(workerThreads));
    
    WorkflowClient mockClient = Mockito.mock(WorkflowClient.class);
    ExecutionWorker worker = new ExecutionWorker(config, mockClient);
    
    Assert.assertEquals(worker.getMaxConcurrentActivityExecutionSize(), workerThreads);
    Assert.assertEquals(worker.getMaxConcurrentLocalActivityExecutionSize(), workerThreads);
    Assert.assertEquals(worker.getMaxConcurrentWorkflowTaskExecutionSize(), workerThreads);
  }

  /**
   * Tests that concurrency fields use default value when no config is set.
   */
  @Test
  public void testConcurrencyFieldsWithDefaultValue() {
    Config config = ConfigFactory.empty();
    
    WorkflowClient mockClient = Mockito.mock(WorkflowClient.class);
    ExecutionWorker worker = new ExecutionWorker(config, mockClient);
    
    int defaultValue = GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER;
    Assert.assertEquals(worker.getMaxConcurrentActivityExecutionSize(), defaultValue);
    Assert.assertEquals(worker.getMaxConcurrentLocalActivityExecutionSize(), defaultValue);
    Assert.assertEquals(worker.getMaxConcurrentWorkflowTaskExecutionSize(), defaultValue);
  }

  /**
   * Tests that specific configs take precedence over base execution worker config.
   */
  @Test
  public void testSpecificConfigTakesPrecedence() {
    int executionWorkerThreads = 20;
    int specificActivityConcurrency = 10;
    
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_EXECUTION_WORKER,
            ConfigValueFactory.fromAnyRef(executionWorkerThreads))
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_EXECUTION_MAX_CONCURRENT_ACTIVITY_SIZE,
            ConfigValueFactory.fromAnyRef(specificActivityConcurrency));
    
    WorkflowClient mockClient = Mockito.mock(WorkflowClient.class);
    ExecutionWorker worker = new ExecutionWorker(config, mockClient);
    
    // Activity should use specific config
    Assert.assertEquals(worker.getMaxConcurrentActivityExecutionSize(), specificActivityConcurrency);
    // Others should fall back to execution worker threads
    Assert.assertEquals(worker.getMaxConcurrentLocalActivityExecutionSize(), executionWorkerThreads);
    Assert.assertEquals(worker.getMaxConcurrentWorkflowTaskExecutionSize(), executionWorkerThreads);
  }
}
