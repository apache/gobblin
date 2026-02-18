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

package org.apache.gobblin.temporal.cluster;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.workflows.service.ManagedWorkflowServiceStubs;


/**
 * Tests for {@link GobblinTemporalTaskRunner} worker initialization logic.
 */
public class GobblinTemporalTaskRunnerTest {

  /**
   * Tests that initializeExecutionWorkers does nothing when dynamic scaling is disabled.
   */
  @Test
  public void testInitializeExecutionWorkersWhenDynamicScalingDisabled() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, ConfigValueFactory.fromAnyRef(false))
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS));

    GobblinTemporalTaskRunner taskRunner = createMockTaskRunner(config);
    List<TemporalWorker> workers = getWorkersField(taskRunner);
    int initialWorkerCount = workers.size();

    invokeInitializeExecutionWorkers(taskRunner);

    Assert.assertEquals(workers.size(), initialWorkerCount,
        "No workers should be added when dynamic scaling is disabled");
  }

  /**
   * Tests that initializeExecutionWorkers does nothing when container is already ExecutionWorker.
   */
  @Test
  public void testInitializeExecutionWorkersWhenAlreadyExecutionWorker() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, ConfigValueFactory.fromAnyRef(true))
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS));

    GobblinTemporalTaskRunner taskRunner = createMockTaskRunner(config);
    List<TemporalWorker> workers = getWorkersField(taskRunner);
    int initialWorkerCount = workers.size();

    invokeInitializeExecutionWorkers(taskRunner);

    Assert.assertEquals(workers.size(), initialWorkerCount,
        "No workers should be added when container is already ExecutionWorker");
  }

  /**
   * Tests that initializeExecutionWorkers does nothing when dynamic scaling config is missing.
   */
  @Test
  public void testInitializeExecutionWorkersWhenConfigMissing() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS));

    GobblinTemporalTaskRunner taskRunner = createMockTaskRunner(config);
    List<TemporalWorker> workers = getWorkersField(taskRunner);
    int initialWorkerCount = workers.size();

    invokeInitializeExecutionWorkers(taskRunner);

    Assert.assertEquals(workers.size(), initialWorkerCount,
        "No workers should be added when dynamic scaling config is missing");
  }

  /**
   * Helper to create a mock GobblinTemporalTaskRunner with necessary fields set.
   */
  private GobblinTemporalTaskRunner createMockTaskRunner(Config config) throws Exception {
    GobblinTemporalTaskRunner taskRunner = Mockito.mock(GobblinTemporalTaskRunner.class, Mockito.CALLS_REAL_METHODS);

    // Set clusterConfig field
    Field clusterConfigField = GobblinTemporalTaskRunner.class.getDeclaredField("clusterConfig");
    clusterConfigField.setAccessible(true);
    clusterConfigField.set(taskRunner, config);

    // Set workers list
    Field workersField = GobblinTemporalTaskRunner.class.getDeclaredField("workers");
    workersField.setAccessible(true);
    workersField.set(taskRunner, new ArrayList<TemporalWorker>());

    // Mock managedWorkflowServiceStubs
    ManagedWorkflowServiceStubs mockStubs = Mockito.mock(ManagedWorkflowServiceStubs.class);
    Field stubsField = GobblinTemporalTaskRunner.class.getDeclaredField("managedWorkflowServiceStubs");
    stubsField.setAccessible(true);
    stubsField.set(taskRunner, mockStubs);

    // Mock WorkflowClient
    Mockito.when(mockStubs.getWorkflowServiceStubs()).thenReturn(null);

    return taskRunner;
  }

  /**
   * Helper to invoke the private initializeExecutionWorkers method using reflection.
   */
  private void invokeInitializeExecutionWorkers(GobblinTemporalTaskRunner taskRunner) throws Exception {
    Method method = GobblinTemporalTaskRunner.class.getDeclaredMethod("initializeExecutionWorkers");
    method.setAccessible(true);
    method.invoke(taskRunner);
  }

  /**
   * Helper to get the workers list field using reflection.
   */
  @SuppressWarnings("unchecked")
  private List<TemporalWorker> getWorkersField(GobblinTemporalTaskRunner taskRunner) throws Exception {
    Field workersField = GobblinTemporalTaskRunner.class.getDeclaredField("workers");
    workersField.setAccessible(true);
    return (List<TemporalWorker>) workersField.get(taskRunner);
  }
}
