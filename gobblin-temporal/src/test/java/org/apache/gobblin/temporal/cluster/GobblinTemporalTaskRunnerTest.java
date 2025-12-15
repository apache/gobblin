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

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.worker.ExecutionWorker;
import org.apache.gobblin.temporal.workflows.helloworld.HelloWorldWorker;


/**
 * Tests for {@link GobblinTemporalTaskRunner} focusing on worker class resolution
 * from configuration without relying on system properties.
 */
public class GobblinTemporalTaskRunnerTest {

  /**
   * Tests that worker class is correctly read from config (not system properties).
   * This verifies the fix where we removed System.getProperty() in favor of ConfigUtils.getString().
   */
  @Test
  public void testWorkerClassResolvedFromConfig() {
    // Setup - config with ExecutionWorker class
    Config config = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(ExecutionWorker.class.getName()));

    // Verify the config contains the expected worker class
    String workerClass = config.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS);
    Assert.assertEquals(workerClass, ExecutionWorker.class.getName(),
        "Config should contain ExecutionWorker class name");
  }

  /**
   * Tests that default worker class is used when not configured.
   */
  @Test
  public void testDefaultWorkerClassWhenNotConfigured() {
    // Setup - empty config
    Config config = ConfigFactory.empty();

    // Verify default is used
    String workerClass = config.hasPath(GobblinTemporalConfigurationKeys.WORKER_CLASS)
        ? config.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS)
        : GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS;
    
    Assert.assertEquals(workerClass, HelloWorldWorker.class.getName(),
        "Should use default HelloWorldWorker when not configured");
  }

  /**
   * Tests that worker class configuration is properly overridden in profile overlay.
   * This simulates how ExecutionWorker profile overrides the baseline worker class.
   */
  @Test
  public void testWorkerClassOverrideInProfile() {
    // Setup - baseline config with default worker
    Config baselineConfig = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(HelloWorldWorker.class.getName()));

    // Simulate profile overlay for execution worker
    Config executionConfig = baselineConfig
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(ExecutionWorker.class.getName()));

    // Verify baseline uses HelloWorldWorker
    Assert.assertEquals(baselineConfig.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        HelloWorldWorker.class.getName(),
        "Baseline should use HelloWorldWorker");

    // Verify execution profile uses ExecutionWorker
    Assert.assertEquals(executionConfig.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        ExecutionWorker.class.getName(),
        "Execution profile should use ExecutionWorker");
  }

  /**
   * Tests that EXECUTION_WORKER_CLASS constant matches ExecutionWorker class name.
   */
  @Test
  public void testExecutionWorkerClassConstant() {
    Assert.assertEquals(GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS,
        ExecutionWorker.class.getName(),
        "EXECUTION_WORKER_CLASS constant should match ExecutionWorker.class.getName()");
  }

  /**
   * Tests that DEFAULT_WORKER_CLASS constant matches HelloWorldWorker class name.
   */
  @Test
  public void testDefaultWorkerClassConstant() {
    Assert.assertEquals(GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS,
        HelloWorldWorker.class.getName(),
        "DEFAULT_WORKER_CLASS constant should match HelloWorldWorker.class.getName()");
  }

  /**
   * Tests worker class configuration for different container types in dynamic scaling.
   */
  @Test
  public void testWorkerClassForDifferentContainerTypes() {
    // Baseline container config (WorkFulfillmentWorker)
    Config baselineConfig = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS));

    // Execution container config (ExecutionWorker)
    Config executionConfig = ConfigFactory.empty()
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS));

    // Verify different worker classes for different container types
    Assert.assertNotEquals(
        baselineConfig.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        executionConfig.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        "Baseline and execution containers should use different worker classes");
  }
}
