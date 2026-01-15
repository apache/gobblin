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

package org.apache.gobblin.temporal.ddm.activity.impl;

import java.util.List;
import java.util.Properties;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.TimeBudget;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;
import org.apache.gobblin.temporal.dynamic.ProfileDerivation;
import org.apache.gobblin.temporal.dynamic.ProfileOverlay;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;
import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;


/**
 * Tests for {@link AbstractRecommendScalingForWorkUnitsImpl} focusing on profile overlay creation
 * and ExecutionWorker configuration for dynamic scaling.
 */
public class AbstractRecommendScalingForWorkUnitsImplTest {

  private TestableAbstractRecommendScalingForWorkUnitsImpl scalingImpl;
  private Properties jobProps;
  
  @Mock
  private WorkUnitsSizeSummary mockWorkSummary;
  
  @Mock
  private TimeBudget mockTimeBudget;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    scalingImpl = new TestableAbstractRecommendScalingForWorkUnitsImpl();
    jobProps = new Properties();
    // JobState requires job.name and job.id
    jobProps.setProperty("job.name", "TestJob");
    jobProps.setProperty("job.id", "TestJob_123");
  }

  /**
   * Tests that ExecutionWorker class is always added to profile overlay.
   */
  @Test
  public void testProfileOverlayAlwaysIncludesExecutionWorkerClass() {
    JobState jobState = new JobState(jobProps);
    
    ProfileDerivation derivation = scalingImpl.calcProfileDerivation(
        WorkforceProfiles.BASELINE_NAME, null, "TestSource", jobState);

    Assert.assertNotNull(derivation);
    Assert.assertEquals(derivation.getBasisProfileName(), WorkforceProfiles.BASELINE_NAME);
    
    ProfileOverlay overlay = derivation.getOverlay();
    Assert.assertTrue(overlay instanceof ProfileOverlay.Adding);
    
    // Verify ExecutionWorker class is in overlay
    ProfileOverlay.Adding addingOverlay = (ProfileOverlay.Adding) overlay;
    boolean hasExecutionWorkerClass = addingOverlay.getAdditionPairs().stream()
        .anyMatch(kv -> kv.getKey().equals(GobblinTemporalConfigurationKeys.WORKER_CLASS)
            && kv.getValue().equals(GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS));
    
    Assert.assertTrue(hasExecutionWorkerClass,
        "Profile overlay should always include ExecutionWorker class");
  }

  /**
   * Tests that execution-specific memory is added to overlay when configured.
   */
  @Test
  public void testProfileOverlayIncludesExecutionMemoryWhenConfigured() {
    String executionMemory = "32768";
    jobProps.setProperty(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB, executionMemory);
    JobState jobState = new JobState(jobProps);
    
    ProfileDerivation derivation = scalingImpl.calcProfileDerivation(
        WorkforceProfiles.BASELINE_NAME, null, "TestSource", jobState);

    ProfileOverlay overlay = derivation.getOverlay();
    Assert.assertTrue(overlay instanceof ProfileOverlay.Adding);
    
    ProfileOverlay.Adding addingOverlay = (ProfileOverlay.Adding) overlay;
    
    // Verify memory is in overlay
    boolean hasMemoryConfig = addingOverlay.getAdditionPairs().stream()
        .anyMatch(kv -> kv.getKey().equals(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY)
            && kv.getValue().equals(executionMemory));
    
    Assert.assertTrue(hasMemoryConfig,
        "Profile overlay should include execution memory when configured");
  }

  /**
   * Tests that memory is not added to overlay when not configured (falls back to baseline).
   */
  @Test
  public void testProfileOverlayOmitsMemoryWhenNotConfigured() {
    JobState jobState = new JobState(jobProps);
    
    ProfileDerivation derivation = scalingImpl.calcProfileDerivation(
        WorkforceProfiles.BASELINE_NAME, null, "TestSource", jobState);

    ProfileOverlay overlay = derivation.getOverlay();
    Assert.assertTrue(overlay instanceof ProfileOverlay.Adding);
    
    ProfileOverlay.Adding addingOverlay = (ProfileOverlay.Adding) overlay;
    
    // Verify memory is NOT in overlay (will use baseline memory)
    boolean hasMemoryConfig = addingOverlay.getAdditionPairs().stream()
        .anyMatch(kv -> kv.getKey().equals(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY));
    
    Assert.assertFalse(hasMemoryConfig,
        "Profile overlay should not include memory config when not set (uses baseline)");
  }

  /**
   * Tests that overlay contains both ExecutionWorker class and memory when both configured.
   */
  @Test
  public void testProfileOverlayIncludesBothWorkerClassAndMemory() {
    String executionMemory = "65536";
    jobProps.setProperty(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB, executionMemory);
    JobState jobState = new JobState(jobProps);
    
    ProfileDerivation derivation = scalingImpl.calcProfileDerivation(
        WorkforceProfiles.BASELINE_NAME, null, "TestSource", jobState);

    ProfileOverlay overlay = derivation.getOverlay();
    Assert.assertTrue(overlay instanceof ProfileOverlay.Adding);
    
    ProfileOverlay.Adding addingOverlay = (ProfileOverlay.Adding) overlay;
    List<ProfileOverlay.KVPair> kvPairs = addingOverlay.getAdditionPairs();
    
    // Should have exactly 2 entries: worker class + memory
    Assert.assertEquals(kvPairs.size(), 2,
        "Overlay should have 2 entries when memory is configured");
    
    boolean hasWorkerClass = kvPairs.stream()
        .anyMatch(kv -> kv.getKey().equals(GobblinTemporalConfigurationKeys.WORKER_CLASS));
    boolean hasMemory = kvPairs.stream()
        .anyMatch(kv -> kv.getKey().equals(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY));
    
    Assert.assertTrue(hasWorkerClass && hasMemory,
        "Overlay should contain both worker class and memory");
  }

  /**
   * Tests that profile derivation name uses default.
   */
  @Test
  public void testProfileDerivationNameUsesDefault() {
    JobState jobState = new JobState(jobProps);
    
    String derivationName = scalingImpl.calcProfileDerivationName(jobState);
    
    Assert.assertEquals(derivationName, AbstractRecommendScalingForWorkUnitsImpl.DEFAULT_PROFILE_DERIVATION_NAME);
  }

  /**
   * Tests that basis profile name is always baseline.
   */
  @Test
  public void testBasisProfileNameIsAlwaysBaseline() {
    JobState jobState = new JobState(jobProps);
    
    String basisProfileName = scalingImpl.calcBasisProfileName(jobState);
    
    Assert.assertEquals(basisProfileName, WorkforceProfiles.BASELINE_NAME);
  }

  /**
   * Tests that recommendScaling returns a ScalingDirective with correct profile derivation.
   */
  @Test
  public void testRecommendScalingReturnsDirectiveWithProfileDerivation() {
    String executionMemory = "16384";
    jobProps.setProperty(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB, executionMemory);
    
    List<ScalingDirective> directives = scalingImpl.recommendScaling(
        mockWorkSummary, "TestSource", mockTimeBudget, jobProps);

    Assert.assertNotNull(directives);
    Assert.assertEquals(directives.size(), 1);
    
    ScalingDirective directive = directives.get(0);
    Assert.assertTrue(directive.getOptDerivedFrom().isPresent(),
        "Directive should have profile derivation");
    
    ProfileDerivation derivation = directive.getOptDerivedFrom().get();
    Assert.assertEquals(derivation.getBasisProfileName(), WorkforceProfiles.BASELINE_NAME);
    
    // Verify overlay has ExecutionWorker class
    ProfileOverlay overlay = derivation.getOverlay();
    Assert.assertTrue(overlay instanceof ProfileOverlay.Adding);
    
    ProfileOverlay.Adding addingOverlay = (ProfileOverlay.Adding) overlay;
    boolean hasExecutionWorker = addingOverlay.getAdditionPairs().stream()
        .anyMatch(kv -> kv.getKey().equals(GobblinTemporalConfigurationKeys.WORKER_CLASS)
            && kv.getValue().equals(GobblinTemporalConfigurationKeys.EXECUTION_WORKER_CLASS));
    
    Assert.assertTrue(hasExecutionWorker,
        "Scaling directive should include ExecutionWorker in profile derivation");
  }

  /**
   * Tests that different memory values create different overlays.
   */
  @Test
  public void testDifferentMemoryValuesCreateDifferentOverlays() {
    // First config with 16GB
    jobProps.setProperty(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB, "16384");
    JobState jobState1 = new JobState(jobProps);
    ProfileDerivation derivation1 = scalingImpl.calcProfileDerivation(
        WorkforceProfiles.BASELINE_NAME, null, "TestSource", jobState1);

    // Second config with 32GB
    Properties jobProps2 = new Properties();
    jobProps2.setProperty("job.name", "TestJob");
    jobProps2.setProperty("job.id", "TestJob_456");
    jobProps2.setProperty(GobblinTemporalConfigurationKeys.WORK_EXECUTION_MEMORY_MB, "32768");
    JobState jobState2 = new JobState(jobProps2);
    ProfileDerivation derivation2 = scalingImpl.calcProfileDerivation(
        WorkforceProfiles.BASELINE_NAME, null, "TestSource", jobState2);

    // Extract memory values from overlays
    ProfileOverlay.Adding overlay1 = (ProfileOverlay.Adding) derivation1.getOverlay();
    ProfileOverlay.Adding overlay2 = (ProfileOverlay.Adding) derivation2.getOverlay();
    
    String memory1 = overlay1.getAdditionPairs().stream()
        .filter(kv -> kv.getKey().equals(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY))
        .map(kv -> kv.getValue())
        .findFirst()
        .orElse(null);
    
    String memory2 = overlay2.getAdditionPairs().stream()
        .filter(kv -> kv.getKey().equals(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY))
        .map(kv -> kv.getValue())
        .findFirst()
        .orElse(null);
    
    Assert.assertEquals(memory1, "16384");
    Assert.assertEquals(memory2, "32768");
    Assert.assertNotEquals(memory1, memory2,
        "Different memory configs should produce different overlay values");
  }

  /**
   * Testable concrete implementation of AbstractRecommendScalingForWorkUnitsImpl.
   */
  private static class TestableAbstractRecommendScalingForWorkUnitsImpl 
      extends AbstractRecommendScalingForWorkUnitsImpl {
    
    @Override
    protected int calcDerivationSetPoint(WorkUnitsSizeSummary remainingWork, String sourceClass,
        TimeBudget timeBudget, JobState jobState) {
      // Simple test implementation: return 5 containers
      return 5;
    }
  }
}
