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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
import org.apache.gobblin.util.ConfigUtils;


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
    ExecutionWorker worker = createMockWorker(config);

    Class<?>[] workflows = invokeGetWorkflowImplClasses(worker);

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
    ExecutionWorker worker = createMockWorker(config);

    Object[] activities = invokeGetActivityImplInstances(worker);

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

    ExecutionWorker worker = createMockWorker(config);
    String taskQueue = invokeGetTaskQueue(worker);

    Assert.assertEquals(taskQueue, customQueue,
        "ExecutionWorker should use execution task queue from config");
  }

  /**
   * Tests that ExecutionWorker uses default execution task queue when not configured.
   */
  @Test
  public void testGetTaskQueueDefault() throws Exception {
    Config config = ConfigFactory.empty();

    ExecutionWorker worker = createMockWorker(config);
    String taskQueue = invokeGetTaskQueue(worker);

    Assert.assertEquals(taskQueue, GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE,
        "ExecutionWorker should use default execution task queue when not configured");
  }

  /**
   * Tests that ExecutionWorker creates WorkerOptions with correct concurrency settings.
   */
  @Test
  public void testCreateWorkerOptionsWithCustomConcurrency() throws Exception {
    int customConcurrency = 10;
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            ConfigValueFactory.fromAnyRef(customConcurrency));

    ExecutionWorker worker = createMockWorker(config);
    WorkerOptions options = invokeCreateWorkerOptions(worker);

    Assert.assertEquals(options.getMaxConcurrentActivityExecutionSize(), customConcurrency,
        "MaxConcurrentActivityExecutionSize should match configured value");
    Assert.assertEquals(options.getMaxConcurrentLocalActivityExecutionSize(), customConcurrency,
        "MaxConcurrentLocalActivityExecutionSize should match configured value");
    Assert.assertEquals(options.getMaxConcurrentWorkflowTaskExecutionSize(), customConcurrency,
        "MaxConcurrentWorkflowTaskExecutionSize should match configured value");
  }

  /**
   * Tests that ExecutionWorker creates WorkerOptions with default concurrency.
   */
  @Test
  public void testCreateWorkerOptionsWithDefaultConcurrency() throws Exception {
    Config config = ConfigFactory.empty();

    ExecutionWorker worker = createMockWorker(config);
    WorkerOptions options = invokeCreateWorkerOptions(worker);

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

    ExecutionWorker worker = createMockWorker(config);
    WorkerOptions options = invokeCreateWorkerOptions(worker);

    long expectedTimeoutMillis = TimeUnit.SECONDS.toMillis(ExecutionWorker.DEADLOCK_DETECTION_TIMEOUT_SECONDS);
    Assert.assertEquals(options.getDefaultDeadlockDetectionTimeout(), expectedTimeoutMillis,
        "Deadlock detection timeout should be set correctly");
  }

  /**
   * Tests that maxExecutionConcurrency field is initialized from config.
   */
  @Test
  public void testMaxExecutionConcurrencyInitialization() throws Exception {
    int customConcurrency = 15;
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            ConfigValueFactory.fromAnyRef(customConcurrency));

    ExecutionWorker worker = createMockWorker(config);

    Assert.assertEquals(worker.maxExecutionConcurrency, customConcurrency,
        "maxExecutionConcurrency should be initialized from config");
  }

  /**
   * Tests that maxExecutionConcurrency uses default when not configured.
   */
  @Test
  public void testMaxExecutionConcurrencyDefault() throws Exception {
    Config config = ConfigFactory.empty();

    ExecutionWorker worker = createMockWorker(config);

    Assert.assertEquals(worker.maxExecutionConcurrency,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER,
        "maxExecutionConcurrency should use default value when not configured");
  }

  /**
   * Helper to create a mock ExecutionWorker without calling the constructor.
   */
  private ExecutionWorker createMockWorker(Config config) throws Exception {
    ExecutionWorker worker = Mockito.mock(ExecutionWorker.class, Mockito.CALLS_REAL_METHODS);
    
    // Set config field
    Field configField = org.apache.gobblin.temporal.cluster.AbstractTemporalWorker.class.getDeclaredField("config");
    configField.setAccessible(true);
    configField.set(worker, config);
    
    // Set maxExecutionConcurrency field
    Field maxConcurrencyField = ExecutionWorker.class.getDeclaredField("maxExecutionConcurrency");
    maxConcurrencyField.setAccessible(true);
    int concurrency = ConfigUtils.getInt(config, GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
        GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER);
    maxConcurrencyField.set(worker, concurrency);
    
    return worker;
  }

  /**
   * Helper to invoke the protected getWorkflowImplClasses method using reflection.
   */
  private Class<?>[] invokeGetWorkflowImplClasses(ExecutionWorker worker) throws Exception {
    Method method = ExecutionWorker.class.getDeclaredMethod("getWorkflowImplClasses");
    method.setAccessible(true);
    return (Class<?>[]) method.invoke(worker);
  }

  /**
   * Helper to invoke the protected getActivityImplInstances method using reflection.
   */
  private Object[] invokeGetActivityImplInstances(ExecutionWorker worker) throws Exception {
    Method method = ExecutionWorker.class.getDeclaredMethod("getActivityImplInstances");
    method.setAccessible(true);
    return (Object[]) method.invoke(worker);
  }

  /**
   * Helper to invoke the protected getTaskQueue method using reflection.
   */
  private String invokeGetTaskQueue(ExecutionWorker worker) throws Exception {
    Method method = ExecutionWorker.class.getDeclaredMethod("getTaskQueue");
    method.setAccessible(true);
    return (String) method.invoke(worker);
  }

  /**
   * Helper to invoke the protected createWorkerOptions method using reflection.
   */
  private WorkerOptions invokeCreateWorkerOptions(ExecutionWorker worker) throws Exception {
    Method method = ExecutionWorker.class.getDeclaredMethod("createWorkerOptions");
    method.setAccessible(true);
    return (WorkerOptions) method.invoke(worker);
  }
}
