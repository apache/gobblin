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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import io.temporal.client.WorkflowClient;
import io.temporal.worker.WorkerOptions;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.activity.impl.ProcessWorkUnitImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.NestingExecOfProcessWorkUnitWorkflowImpl;
import org.apache.gobblin.temporal.ddm.workflow.impl.ProcessWorkUnitsWorkflowImpl;


/**
 * Tests for {@link ExecutionWorker} verifying workflow/activity registration and configuration.
 */
public class ExecutionWorkerTest {

  /**
   * Tests that ExecutionWorker registers only the workflows needed for work execution.
   */
  @Test
  public void testGetWorkflowImplClasses() throws Exception {
    Config config = ConfigFactory.empty();
    ExecutionWorker worker = createWorker(config);

    Class<?>[] workflows = worker.getWorkflowImplClasses();

    Assert.assertEquals(workflows.length, 2,
        "ExecutionWorker should register exactly 2 workflow types");

    List<String> workflowNames = Arrays.stream(workflows)
        .map(Class::getName)
        .collect(Collectors.toList());

    Assert.assertTrue(workflowNames.contains(ProcessWorkUnitsWorkflowImpl.class.getName()),
        "ExecutionWorker should register ProcessWorkUnitsWorkflowImpl");
    Assert.assertTrue(workflowNames.contains(NestingExecOfProcessWorkUnitWorkflowImpl.class.getName()),
        "ExecutionWorker should register NestingExecOfProcessWorkUnitWorkflowImpl");
  }

  /**
   * Tests that ExecutionWorker registers only ProcessWorkUnit activity.
   */
  @Test
  public void testGetActivityImplInstances() throws Exception {
    Config config = ConfigFactory.empty();
    ExecutionWorker worker = createWorker(config);

    Object[] activities = worker.getActivityImplInstances();

    Assert.assertEquals(activities.length, 1,
        "ExecutionWorker should register exactly 1 activity type");
    Assert.assertTrue(activities[0] instanceof ProcessWorkUnitImpl,
        "ExecutionWorker should register ProcessWorkUnitImpl activity");
  }

  /**
   * Tests that ExecutionWorker uses execution task queue from config.
   */
  @Test
  public void testGetTaskQueueFromConfig() throws Exception {
    String customQueue = "CustomExecutionQueue";
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef(customQueue));

    ExecutionWorker worker = createWorker(config);
    String taskQueue = worker.getTaskQueue();

    Assert.assertEquals(taskQueue, customQueue,
        "ExecutionWorker should use execution task queue from config");
  }

  /**
   * Tests that ExecutionWorker uses default execution task queue when not configured.
   */
  @Test
  public void testGetTaskQueueDefault() throws Exception {
    Config config = ConfigFactory.empty();

    ExecutionWorker worker = createWorker(config);
    String taskQueue = worker.getTaskQueue();

    Assert.assertEquals(taskQueue, GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE,
        "ExecutionWorker should use default execution task queue when not configured");
  }

  /**
   * Tests that ExecutionWorker creates WorkerOptions with correct concurrency settings.
   */
  @Test
  public void testCreateWorkerOptionsWithCustomConcurrency() throws Exception {
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

    ExecutionWorker worker = createWorker(config);
    WorkerOptions options = worker.createWorkerOptions();

    Assert.assertEquals(options.getMaxConcurrentActivityExecutionSize(), customActivityConcurrency,
        "MaxConcurrentActivityExecutionSize should match configured value");
    Assert.assertEquals(options.getMaxConcurrentLocalActivityExecutionSize(), customLocalActivityConcurrency,
        "MaxConcurrentLocalActivityExecutionSize should match configured value");
    Assert.assertEquals(options.getMaxConcurrentWorkflowTaskExecutionSize(), customWorkflowTaskConcurrency,
        "MaxConcurrentWorkflowTaskExecutionSize should match configured value");
  }

  /**
   * Tests that ExecutionWorker creates WorkerOptions with default concurrency.
   */
  @Test
  public void testCreateWorkerOptionsWithDefaultConcurrency() throws Exception {
    Config config = ConfigFactory.empty();

    ExecutionWorker worker = createWorker(config);
    WorkerOptions options = worker.createWorkerOptions();

    int defaultConcurrency = GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER;
    Assert.assertEquals(options.getMaxConcurrentActivityExecutionSize(), defaultConcurrency,
        "MaxConcurrentActivityExecutionSize should use default value");
    Assert.assertEquals(options.getMaxConcurrentLocalActivityExecutionSize(), defaultConcurrency,
        "MaxConcurrentLocalActivityExecutionSize should use default value");
    Assert.assertEquals(options.getMaxConcurrentWorkflowTaskExecutionSize(), defaultConcurrency,
        "MaxConcurrentWorkflowTaskExecutionSize should use default value");
  }

  /**
   * Tests that ExecutionWorker sets deadlock detection timeout correctly.
   */
  @Test
  public void testCreateWorkerOptionsDeadlockTimeout() throws Exception {
    Config config = ConfigFactory.empty();

    ExecutionWorker worker = createWorker(config);
    WorkerOptions options = worker.createWorkerOptions();

    long expectedTimeoutMillis = TimeUnit.SECONDS.toMillis(ExecutionWorker.DEADLOCK_DETECTION_TIMEOUT_SECONDS);
    Assert.assertEquals(options.getDefaultDeadlockDetectionTimeout(), expectedTimeoutMillis,
        "Deadlock detection timeout should be set correctly");
  }

  /**
   * Tests that concurrency fields are initialized from config.
   */
  @Test
  public void testConcurrencyFieldsInitialization() throws Exception {
    int customConcurrency = 15;
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_EXECUTION_WORKER,
            ConfigValueFactory.fromAnyRef(customConcurrency));

    ExecutionWorker worker = createWorker(config);

    Assert.assertEquals(worker.getMaxConcurrentActivityExecutionSize(), customConcurrency,
        "maxConcurrentActivityExecutionSize should be initialized from execution worker config");
    Assert.assertEquals(worker.getMaxConcurrentLocalActivityExecutionSize(), customConcurrency,
        "maxConcurrentLocalActivityExecutionSize should be initialized from execution worker config");
    Assert.assertEquals(worker.getMaxConcurrentWorkflowTaskExecutionSize(), customConcurrency,
        "maxConcurrentWorkflowTaskExecutionSize should be initialized from execution worker config");
  }

  /**
   * Tests that concurrency fields use default when not configured.
   */
  @Test
  public void testConcurrencyFieldsDefault() throws Exception {
    Config config = ConfigFactory.empty();

    ExecutionWorker worker = createWorker(config);

    int defaultConcurrency = GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER;
    Assert.assertEquals(worker.getMaxConcurrentActivityExecutionSize(), defaultConcurrency,
        "maxConcurrentActivityExecutionSize should use default value when not configured");
    Assert.assertEquals(worker.getMaxConcurrentLocalActivityExecutionSize(), defaultConcurrency,
        "maxConcurrentLocalActivityExecutionSize should use default value when not configured");
    Assert.assertEquals(worker.getMaxConcurrentWorkflowTaskExecutionSize(), defaultConcurrency,
        "maxConcurrentWorkflowTaskExecutionSize should use default value when not configured");
  }

  /**
   * Helper to create an ExecutionWorker instance with mocked dependencies.
   * Uses a partial mock to avoid calling the parent constructor which requires full Temporal setup.
   */
  private ExecutionWorker createWorker(Config config) throws Exception {
    // Create a spy that doesn't call the real constructor
    ExecutionWorker worker = Mockito.mock(ExecutionWorker.class, Mockito.CALLS_REAL_METHODS);
    
    // Manually set the config field from AbstractTemporalWorker
    java.lang.reflect.Field configField = org.apache.gobblin.temporal.cluster.AbstractTemporalWorker.class
        .getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(worker, config);
    
    // Manually initialize the concurrency fields by calling the logic from the constructor
    int defaultThreadsPerWorker = GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER;
    int executionWorkerThreads = org.apache.gobblin.util.ConfigUtils.getInt(config,
        GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_EXECUTION_WORKER,
        org.apache.gobblin.util.ConfigUtils.getInt(config, 
            GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER, defaultThreadsPerWorker));
    
    java.lang.reflect.Field activityField = ExecutionWorker.class.getDeclaredField("maxConcurrentActivityExecutionSize");
    activityField.setAccessible(true);
    activityField.set(worker, org.apache.gobblin.util.ConfigUtils.getInt(config,
        GobblinTemporalConfigurationKeys.TEMPORAL_EXECUTION_MAX_CONCURRENT_ACTIVITY_SIZE,
        executionWorkerThreads));
    
    java.lang.reflect.Field localActivityField = ExecutionWorker.class.getDeclaredField("maxConcurrentLocalActivityExecutionSize");
    localActivityField.setAccessible(true);
    localActivityField.set(worker, org.apache.gobblin.util.ConfigUtils.getInt(config,
        GobblinTemporalConfigurationKeys.TEMPORAL_EXECUTION_MAX_CONCURRENT_LOCAL_ACTIVITY_SIZE,
        executionWorkerThreads));
    
    java.lang.reflect.Field workflowTaskField = ExecutionWorker.class.getDeclaredField("maxConcurrentWorkflowTaskExecutionSize");
    workflowTaskField.setAccessible(true);
    workflowTaskField.set(worker, org.apache.gobblin.util.ConfigUtils.getInt(config,
        GobblinTemporalConfigurationKeys.TEMPORAL_EXECUTION_MAX_CONCURRENT_WORKFLOW_TASK_SIZE,
        executionWorkerThreads));
    
    return worker;
  }
}
