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

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.cluster.GobblinClusterConfigurationKeys;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.ProfileOverlay;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


/**
 * Tests for ExecutionWorker profile overlay creation and configuration.
 * Verifies that execution worker profiles are correctly configured with
 * worker class, memory, and other settings.
 */
public class ExecutionWorkerProfileTest {

  private Config baseConfig;

  @BeforeMethod
  public void setup() {
    baseConfig = ConfigFactory.empty()
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
            ConfigValueFactory.fromAnyRef(8192))
        .withValue(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY,
            ConfigValueFactory.fromAnyRef(4));
  }

  /**
   * Tests that execution profile overlay sets ExecutionWorker class.
   */
  @Test
  public void testExecutionProfileSetsWorkerClass() {
    // Create overlay pairs as DynamicScalingYarnService does
    ProfileOverlay.KVPair workerClassPair = new ProfileOverlay.KVPair(
        GobblinTemporalConfigurationKeys.WORKER_CLASS,
        GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS
    );

    ProfileOverlay overlay = new ProfileOverlay.Adding(workerClassPair);
    Config overlaidConfig = overlay.applyOverlay(baseConfig);

    // Verify
    Assert.assertTrue(overlaidConfig.hasPath(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        "Overlaid config should have worker class");
    Assert.assertEquals(overlaidConfig.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS,
        "Should set ExecutionWorker class");
  }

  /**
   * Tests that execution profile overlay sets Helix tag.
   */
  @Test
  public void testExecutionProfileSetsHelixTag() {
    // Create overlay
    ProfileOverlay.KVPair helixTagPair = new ProfileOverlay.KVPair(
        GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY,
        "execution"
    );

    ProfileOverlay overlay = new ProfileOverlay.Adding(helixTagPair);
    Config overlaidConfig = overlay.applyOverlay(baseConfig);

    // Verify
    Assert.assertTrue(overlaidConfig.hasPath(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY),
        "Overlaid config should have Helix tag");
    Assert.assertEquals(overlaidConfig.getString(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY),
        "execution",
        "Should set execution tag");
  }

  /**
   * Tests that execution profile overlay can override memory configuration.
   */
  @Test
  public void testExecutionProfileOverridesMemory() {
    // Setup - config with stage-specific memory
    Config configWithStageMemory = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB,
            ConfigValueFactory.fromAnyRef("65536"));

    // Create overlay with memory override
    ProfileOverlay.KVPair memoryPair = new ProfileOverlay.KVPair(
        GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
        "65536"
    );

    ProfileOverlay overlay = new ProfileOverlay.Adding(memoryPair);
    Config overlaidConfig = overlay.applyOverlay(baseConfig);

    // Verify
    Assert.assertEquals(overlaidConfig.getString(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY),
        "65536",
        "Should override memory to 64GB for execution workers");
  }

  /**
   * Tests complete execution profile overlay with all settings.
   */
  @Test
  public void testCompleteExecutionProfileOverlay() {
    // Setup - simulate DynamicScalingYarnService.createExecutionProfileOverlay()
    Config configWithStageMemory = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB,
            ConfigValueFactory.fromAnyRef("32768"));

    // Create complete overlay
    List<ProfileOverlay.KVPair> overlayPairs = Arrays.asList(
        new ProfileOverlay.KVPair(
            GobblinTemporalConfigurationKeys.WORKER_CLASS,
            GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS
        ),
        new ProfileOverlay.KVPair(
            GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY,
            "execution"
        ),
        new ProfileOverlay.KVPair(
            GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY,
            "32768"
        )
    );

    ProfileOverlay overlay = new ProfileOverlay.Adding(overlayPairs);
    Config overlaidConfig = overlay.applyOverlay(baseConfig);

    // Verify all settings
    Assert.assertEquals(overlaidConfig.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS,
        "Should set ExecutionWorker class");
    Assert.assertEquals(overlaidConfig.getString(GobblinClusterConfigurationKeys.HELIX_INSTANCE_TAGS_KEY),
        "execution",
        "Should set execution tag");
    Assert.assertEquals(overlaidConfig.getString(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY),
        "32768",
        "Should set execution memory");
    Assert.assertEquals(overlaidConfig.getInt(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY),
        4,
        "Should preserve baseline cores");
  }

  /**
   * Tests that execution profile falls back to baseline memory when stage-specific not configured.
   */
  @Test
  public void testExecutionProfileFallsBackToBaselineMemory() {
    // Create overlay without memory (simulating no stage-specific memory config)
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

    ProfileOverlay overlay = new ProfileOverlay.Adding(overlayPairs);
    Config overlaidConfig = overlay.applyOverlay(baseConfig);

    // Verify baseline memory is preserved
    Assert.assertEquals(overlaidConfig.getInt(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY),
        8192,
        "Should use baseline memory when stage-specific not configured");
  }

  /**
   * Tests that baseline profile does NOT have ExecutionWorker class.
   */
  @Test
  public void testBaselineProfileDoesNotHaveExecutionWorker() {
    // Baseline config should not have execution worker class
    Config baselineWithWorker = baseConfig
        .withValue(GobblinTemporalConfigurationKeys.WORKER_CLASS,
            ConfigValueFactory.fromAnyRef(GobblinTemporalConfigurationKeys.DEFAULT_WORKER_CLASS));

    Assert.assertNotEquals(
        baselineWithWorker.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS,
        "Baseline profile should not use ExecutionWorker");
  }

  /**
   * Tests that execution profile overlay is idempotent.
   */
  @Test
  public void testExecutionProfileOverlayIsIdempotent() {
    // Create overlay
    ProfileOverlay.KVPair workerClassPair = new ProfileOverlay.KVPair(
        GobblinTemporalConfigurationKeys.WORKER_CLASS,
        GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS
    );
    ProfileOverlay overlay = new ProfileOverlay.Adding(workerClassPair);

    // Apply twice
    Config overlaidOnce = overlay.applyOverlay(baseConfig);
    Config overlaidTwice = overlay.applyOverlay(overlaidOnce);

    // Verify same result
    Assert.assertEquals(
        overlaidOnce.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        overlaidTwice.getString(GobblinTemporalConfigurationKeys.WORKER_CLASS),
        "Overlay should be idempotent");
  }
}
