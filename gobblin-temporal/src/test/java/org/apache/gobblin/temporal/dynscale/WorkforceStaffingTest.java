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

package org.apache.gobblin.temporal.dynscale;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

import static org.mockito.ArgumentMatchers.anyString;


public class WorkforceStaffingTest {

  @Mock private WorkforceProfiles profiles;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    Mockito.when(profiles.getOrThrow(anyString())).thenAnswer(invocation ->
        new WorkerProfile(invocation.getArgument(0), null));
  }

  @Test
  public void initializeShouldSetInitialBaselineSetPoint() {
    int initialBaselineSetPoint = 5;
    WorkforceStaffing staffing = WorkforceStaffing.initialize(initialBaselineSetPoint);
    Assert.assertEquals(staffing.getStaffing(WorkforceProfiles.BASELINE_NAME), Optional.of(initialBaselineSetPoint));
  }

  @Test
  public void reviseStaffingShouldUpdateSetPoint() {
    String profileName = "testProfile";
    WorkforceStaffing staffing = WorkforceStaffing.initialize(0);
    staffing.reviseStaffing(profileName, 10, 1000L);
    Assert.assertEquals(staffing.getStaffing(profileName), Optional.of(10));

    staffing.reviseStaffing(profileName, 17, 2000L);
    Assert.assertEquals(staffing.getStaffing(profileName), Optional.of(17));
  }

  @Test
  public void calcDeltasShouldReturnCorrectDeltas() {
    String subsequentlyUnreferencedProfileName = "unreferenced";
    String newlyAddedProfileName = "added";
    String heldSteadyProfileName = "steady";
    WorkforceStaffing currentStaffing = WorkforceStaffing.initialize(5);
    currentStaffing.reviseStaffing(subsequentlyUnreferencedProfileName, 3, 1000L);
    currentStaffing.reviseStaffing(heldSteadyProfileName, 9, 2000L);

    WorkforceStaffing improvedStaffing = WorkforceStaffing.initialize(7);
    improvedStaffing.reviseStaffing(newlyAddedProfileName, 10, 3000L);
    improvedStaffing.reviseStaffing(heldSteadyProfileName, 9, 4000L);

    StaffingDeltas deltas = improvedStaffing.calcDeltas(currentStaffing, profiles);
    Assert.assertEquals(deltas.getPerProfileDeltas().size(), 3);
    // validate every delta
    Map<String, Integer> deltaByProfileName = deltas.getPerProfileDeltas().stream()
        .collect(Collectors.toMap(delta -> delta.getProfile().getName(), StaffingDeltas.ProfileDelta::getDelta));
    ImmutableMap<String, Integer> expectedDeltaByProfileName = ImmutableMap.of(
        WorkforceProfiles.BASELINE_NAME, 2,
        subsequentlyUnreferencedProfileName, -3,
        // NOTE: NOT present (when delta == 0)!
        // heldSteadyProfileName, 0,
        newlyAddedProfileName, 10
    );
    Assert.assertEqualsNoOrder(deltaByProfileName.keySet().toArray(), expectedDeltaByProfileName.keySet().toArray());
    Assert.assertEquals(deltaByProfileName.get(WorkforceProfiles.BASELINE_NAME), expectedDeltaByProfileName.get(WorkforceProfiles.BASELINE_NAME));
    Assert.assertEquals(deltaByProfileName.get(subsequentlyUnreferencedProfileName), expectedDeltaByProfileName.get(subsequentlyUnreferencedProfileName));
    Assert.assertEquals(deltaByProfileName.get(newlyAddedProfileName), expectedDeltaByProfileName.get(newlyAddedProfileName));
  }
}
