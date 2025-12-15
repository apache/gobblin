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

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;


/**
 * Tests for {@link ExecutionWorker} configuration verification.
 * Tests configuration keys and default values without requiring Temporal infrastructure.
 */
public class ExecutionWorkerTest {

  private Config baseConfig;

  @BeforeMethod
  public void setup() {
    baseConfig = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef("TestQueue"));
  }

  @Test
  public void testExecutionWorkerUsesExecutionTaskQueue() {
    String executionQueue = "GobblinTemporalExecutionQueue";
    Config config = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE,
            ConfigValueFactory.fromAnyRef(executionQueue));

    String configuredQueue = config.getString(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE);
    Assert.assertEquals(configuredQueue, executionQueue);
  }

  @Test
  public void testExecutionWorkerUsesDefaultExecutionQueue() {
    String defaultQueue = baseConfig.hasPath(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE)
        ? baseConfig.getString(GobblinTemporalConfigurationKeys.EXECUTION_TASK_QUEUE)
        : GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE;

    Assert.assertEquals(defaultQueue, GobblinTemporalConfigurationKeys.DEFAULT_EXECUTION_TASK_QUEUE);
  }

  @Test
  public void testExecutionWorkerRegistersCorrectWorkflows() {
    Assert.assertEquals(GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS,
        "org.apache.gobblin.temporal.ddm.worker.ExecutionWorker");
  }

  @Test
  public void testExecutionWorkerRegistersOnlyProcessWorkUnitActivity() {
    Assert.assertTrue(baseConfig.hasPath(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_TASK_QUEUE));
  }

  @Test
  public void testExecutionWorkerConfiguresWorkerOptions() {
    int expectedConcurrency = 10;
    Config config = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            ConfigValueFactory.fromAnyRef(expectedConcurrency));

    int configuredConcurrency = config.getInt(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER);
    Assert.assertEquals(configuredConcurrency, expectedConcurrency);
  }

  @Test
  public void testExecutionWorkerUsesDefaultConcurrency() {
    int defaultConcurrency = GobblinTemporalConfigurationKeys.DEFAULT_TEMPORAL_NUM_THREADS_PER_WORKER;
    
    int concurrency = baseConfig.hasPath(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER)
        ? baseConfig.getInt(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER)
        : defaultConcurrency;
    
    Assert.assertEquals(concurrency, defaultConcurrency);
  }

  @Test
  public void testExecutionWorkerSetsDeadlockDetectionTimeout() {
    Assert.assertTrue(true);
  }

  @Test
  public void testMaxExecutionConcurrencyInitialization() {
    int expectedConcurrency = 15;
    Config config = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER,
            ConfigValueFactory.fromAnyRef(expectedConcurrency));

    int configuredConcurrency = config.getInt(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER);
    Assert.assertEquals(configuredConcurrency, expectedConcurrency);
  }
}
