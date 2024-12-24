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
  public void testCalcDerivationSetPoint() {
    Mockito.when(jobState.getPropAsInt(Mockito.eq(GobblinTemporalConfigurationKeys.TEMPORAL_NUM_WORKERS_PER_CONTAINER), Mockito.anyInt()))
        .thenReturn(4); // 4 workers per container
    Mockito.when(jobState.getPropAsLong(Mockito.eq(RecommendScalingForWorkUnitsLinearHeuristicImpl.AMORTIZED_NUM_BYTES_PER_MINUTE), Mockito.anyLong()))
        .thenReturn(100L * 1000 * 1000); // 100MB/minute
    long targetTimeBudgetMinutes = 75L;
    Mockito.when(timeBudget.getMaxTargetDurationMinutes()).thenReturn(targetTimeBudgetMinutes);

    long totalNumMWUs = 3000L;
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(totalNumMWUs);
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsMeanSize()).thenReturn(500e6); // 500MB
    // parallelization capacity = 20 container-slots (= 4 * 5)
    // per-container-slot rate = 5 container-slot-mins/mean(MWU) (= 500 MB/mean(MWU) / 100MB/min)
    long numMWUsPerMinutePerContainer = 4; // (amortized) per-container rate = 4 MWU/container-minute (= 20 / 5)
    long totalNumContainerMinutesAllMWUs = totalNumMWUs / numMWUsPerMinutePerContainer; // 750 container-minutes (= 3000 MWU / 4 MWU/container-min)
    long expectedSetPoint = totalNumContainerMinutesAllMWUs / targetTimeBudgetMinutes; // 10 containers (= 750 / 75)

    int resultA = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "sourceClass", timeBudget, jobState);
    Assert.assertEquals(resultA, expectedSetPoint);

    // verify: 3x MWUs ==> 3x the recommended set point
    Mockito.when(workUnitsSizeSummary.getTopLevelWorkUnitsCount()).thenReturn(totalNumMWUs * 3);
    int tripledResult = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "sourceClass", timeBudget, jobState);
    Assert.assertEquals(tripledResult, resultA * 3);

    // reduce the target duration by a third, and verify: 3/2 the recommended set point
    Mockito.when(timeBudget.getMaxTargetDurationMinutes()).thenReturn(2 * (targetTimeBudgetMinutes / 3));
    int reducedTimeBudgetResult = scalingHeuristic.calcDerivationSetPoint(workUnitsSizeSummary, "sourceClass", timeBudget, jobState);
    Assert.assertEquals(reducedTimeBudgetResult, (long) Math.round(expectedSetPoint * 3 * (3.0 / 2.0)));
  }
}
