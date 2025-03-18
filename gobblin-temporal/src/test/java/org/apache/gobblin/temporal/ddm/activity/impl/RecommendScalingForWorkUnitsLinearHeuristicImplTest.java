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

import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.ddm.work.TimeBudget;
import org.apache.gobblin.temporal.ddm.work.WorkUnitsSizeSummary;


/** Test for {@link RecommendScalingForWorkUnitsLinearHeuristicImpl} */
public class RecommendScalingForWorkUnitsLinearHeuristicImplTest {

  private RecommendScalingForWorkUnitsLinearHeuristicImpl scalingHeuristic;
  @Mock private JobState jobState;
  @Mock private WorkUnitsSizeSummary workUnitsSizeSummary;
  @Mock private TimeBudget timeBudget;

  @BeforeMethod
  public void setUp() {
    scalingHeuristic = new RecommendScalingForWorkUnitsLinearHeuristicImpl();
    MockitoAnnotations.openMocks(this);
  }

  @Test
  void testScalingDisabled() {
    Mockito.when(jobState.getPropAsBoolean(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, false)).thenReturn(false);
    Mockito.when(jobState.getProp(GobblinYarnConfigurationKeys.INITIAL_CONTAINERS_KEY, "1")).thenReturn("2");

    long totalNumMWUs = 3000L;
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(totalNumMWUs);
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsMeanSize()).thenReturn(500e6); // 500MB
    int result = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "someSourceClass", timeBudget, jobState);

    Assert.assertEquals(2, result, "Should return initial container count if dynamic scaling is disabled");
  }

  @Test
  public void testCalcDerivationSetPointForNoWorkUnits() {
    Mockito.when(jobState.getPropAsBoolean(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, false))
        .thenReturn(true);
    stubContainerCapacity(1,5);

    Mockito.when(jobState.getPropAsLong(
            Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE), Mockito.anyLong()))
        .thenReturn(100L * 1000 * 1000);
    Mockito.when(jobState.getPropAsInt(Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKUNITS_PER_CONTAINER), Mockito.anyInt()))
        .thenReturn(200);
    Mockito.when(timeBudget.getMaxTargetDurationMinutes()).thenReturn(60L);

    // Set both top-level and constituent work units to zero.
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(0L);
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsMeanSize()).thenReturn(500e6);
    Mockito.when(workUnitsSizeSummary.getConstituentWorkUnitsCount()).thenReturn(0L);

    int result = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "source", timeBudget, jobState);
    Assert.assertEquals(result, 0, "Expected 0 containers when there are no work units");
  }

  @Test
  public void testCalcDerivationSetPointWithThroughputConstraintDominating() {
    Mockito.when(jobState.getPropAsBoolean(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, false))
        .thenReturn(true);

    stubContainerCapacity(4,5);

    Mockito.when(jobState.getPropAsLong(
            Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE),
            Mockito.anyLong()))
        .thenReturn(50L * 1000 * 1000); // 50MB/minute

    Mockito.when(jobState.getPropAsInt(Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKUNITS_PER_CONTAINER), Mockito.anyInt()))
        .thenReturn(5000);
    Mockito.when(timeBudget.getMaxTargetDurationMinutes()).thenReturn(60L);

    // Configure work units: throughput constraint will dominate.
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(1000L);
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsMeanSize()).thenReturn(500e6);
    Mockito.when(workUnitsSizeSummary.getConstituentWorkUnitsCount()).thenReturn(1500L);

    // Throughput calculation:
    // totalBytes = 1000 * 500e6 = 500e9 bytes.
    // containerProcRate = 50e6 * 20 = 1e9 bytes/min.
    // estimated container minutes = 500e9 / 1e9 = 500 minutes.
    // Throughput containers = ceil(500 / 60) = 9.
    // Work unit constraint = ceil(1500 / 5000) = 1.
    // Expected recommended containers = max(9, 1) = 9.
    int result = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "source", timeBudget, jobState);
    Assert.assertEquals(result, 9, "Expected throughput constraint to dominate and yield 9 containers");
  }

  @Test
  public void testCalcDerivationSetPointWithWorkUnitConstraintDominating2() {
    // Enable dynamic scaling.
    Mockito.when(jobState.getPropAsBoolean(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, false))
        .thenReturn(true);

    stubContainerCapacity(4,5);

    Mockito.when(jobState.getPropAsLong(
            Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE),
            Mockito.anyLong()))
        .thenReturn(100L * 1000 * 1000);

    Mockito.when(jobState.getPropAsInt(Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKUNITS_PER_CONTAINER), Mockito.anyInt()))
        .thenReturn(10);

    Mockito.when(timeBudget.getMaxTargetDurationMinutes()).thenReturn(120L);

    // Configure work units:
    // For throughput: top-level MWUs count and mean size.
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(1000L);
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsMeanSize()).thenReturn(500e6);  // 500MB per MWU
    // For work unit constraint: total constituent work units.
    Mockito.when(workUnitsSizeSummary.getConstituentWorkUnitsCount()).thenReturn(5000L);

    // Throughput calculation:
    // totalBytes = 1000 * 500e6 = 5.0E11 bytes.
    // Container capacity = 4 workers * 5 threads = 20.
    // Container throughput = 100e6 * 20 = 2.0E9 bytes/minute.
    // Estimated container minutes = 5.0E11 / 2.0E9 = 250 minutes.
    // Throughput constraint = ceil(250 / 120) = 3 containers.

    // Work unit constraint = ceil(5000 / 10) = 500 containers.
    // The recommended set point should be max(3, 500) = 500 containers.
    int result = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "source", timeBudget, jobState);
    Assert.assertEquals(result, 500, "Expected work unit constraint to dominate and yield 500 containers");
  }

  @Test
  public void testCalcDerivationSetPointWithChangingTimeBudget() {
    Mockito.when(jobState.getPropAsBoolean(GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_ENABLED, false)).thenReturn(true);
    stubContainerCapacity(4,5);

    // Set throughput using the correct key.
    Mockito.when(jobState.getPropAsLong(
            Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKER_THREAD_AMORTIZED_THROUGHPUT_PER_MINUTE),
            Mockito.anyLong()))
        .thenReturn(100L * 1000 * 1000); // 100 MB/minute per thread

    // Set the work unit constraint so that throughput dominates.
    Mockito.when(jobState.getPropAsInt(
            Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_WORKUNITS_PER_CONTAINER), Mockito.anyInt()))
        .thenReturn(50);
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(50L);


    // Set the target job duration.
    long targetTimeBudgetMinutes = 75L;
    Mockito.when(timeBudget.getMaxTargetDurationMinutes()).thenReturn(targetTimeBudgetMinutes);

    // Configure work units: 3000 top-level work units with an average size of 500 MB.
    long totalNumMWUs = 3000L;
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(totalNumMWUs);
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsMeanSize()).thenReturn(500e6); // 500 MB


    // Calculate expected throughput constraint:
    //   totalBytes = 3000 * 500e6 = 1.5e12 bytes.
    //   Container capacity = 4 * 5 = 20.
    //   Container throughput = 100e6 * 20 = 2e9 bytes/min.
    //   Estimated container minutes = 1.5e12 / 2e9 = 750 minutes.
    //   Throughput constraint = ceil(750 / 75) = 10 containers.
    int expectedThroughputContainers = 10;

    int resultA = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "sourceClass", timeBudget, jobState);
    Assert.assertEquals(resultA, expectedThroughputContainers,
        "Expected recommended containers based on throughput constraint");

    // Verify: Tripling the top-level work units should triple the recommended set point.
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(totalNumMWUs * 3);
    int tripledResult = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "sourceClass", timeBudget, jobState);
    Assert.assertEquals(tripledResult, resultA * 3,
        "Tripling the number of top-level work units should triple the set point");

    // Test reduced time budget: for the tripled workload, reduce the target duration.
    // New target duration = 2 * (75 / 3) = 50 minutes.
    Mockito.when(timeBudget.getMaxTargetDurationMinutes()).thenReturn(2 * (targetTimeBudgetMinutes / 3));
    // Recalculate throughput constraint:
    //   totalBytes = 9000 * 500e6 = 4.5e12 bytes.
    //   Container throughput remains 2e9 bytes/min.
    //   Estimated container minutes = 4.5e12 / 2e9 = 2250 minutes.
    //   Throughput constraint = ceil(2250 / 50) = 45 containers.
    int expectedReducedContainers = 45;
    int reducedTimeBudgetResult = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "sourceClass", timeBudget, jobState);
    Assert.assertEquals(reducedTimeBudgetResult, expectedReducedContainers,
        "Reducing the target duration should increase the recommended set point proportionally");
  }

  /**
   * Stubs the container capacity properties in the {@link JobState} mock.
   *
   * @param numWorkers the number of workers per container to stub in the {@link JobState} mock.
   * @param threadsPerWorker the number of threads per worker to stub in the {@link JobState} mock.
   */
  private void stubContainerCapacity(int numWorkers, int threadsPerWorker) {
    Mockito.when(jobState.getPropAsInt(
            Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_WORKERS_PER_CONTAINER), Mockito.anyInt()))
        .thenReturn(numWorkers);
    Mockito.when(jobState.getPropAsInt(
            Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_THREADS_PER_WORKER), Mockito.anyInt()))
        .thenReturn(threadsPerWorker);
  }

}
