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

package org.apache.gobblin.temporal.dynamic;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;


public class WorkforcePlanTest {
  private Config baselineConfig = ConfigFactory.parseString("key1=value1, key2=value2");
  private final int initialBaselineSetPoint = 10;
  private WorkforcePlan plan;

  @BeforeMethod
  public void setUp() {
    plan = new WorkforcePlan(baselineConfig, initialBaselineSetPoint);
  }

  private static ScalingDirective createNewProfileDirective(String profileName, int setPoint, long epochMillis, String basisProfileName) {
    return new ScalingDirective(profileName, setPoint, epochMillis, Optional.of(
        new ProfileDerivation(basisProfileName, new ProfileOverlay.Adding(Lists.newArrayList(
            new ProfileOverlay.KVPair("key1", "new_value"),
            new ProfileOverlay.KVPair("key4", "value4"))))));
  }

  @Test
  public void reviseWithValidReSetPoint() throws WorkforcePlan.IllegalRevisionException {
    plan.revise(new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 7,10000L));
    plan.revise(new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 1,20000L));
    Assert.assertEquals(plan.getLastRevisionEpochMillis(), 20000L);
    Assert.assertEquals(plan.getNumProfiles(), 1);
  }

  @Test
  public void reviseWithValidDerivation() throws WorkforcePlan.IllegalRevisionException {
    Assert.assertEquals(plan.getLastRevisionEpochMillis(), WorkforceStaffing.INITIALIZATION_PROVENANCE_EPOCH_MILLIS);
    Assert.assertEquals(plan.getNumProfiles(), 1);
    ScalingDirective directive = createNewProfileDirective("new_profile", 5,10000L, WorkforceProfiles.BASELINE_NAME);
    plan.revise(directive);

    Assert.assertEquals(plan.getLastRevisionEpochMillis(), 10000L);
    Assert.assertEquals(plan.getNumProfiles(), 2);
    Config expectedConfig = ConfigFactory.parseString("key1=new_value, key2=value2, key4=value4");
    Assert.assertEquals(plan.peepProfile("new_profile").getConfig(), expectedConfig);
  }

  @Test
  public void reviseWhenNewerIgnoresOutOfOrderDirectives() throws WorkforcePlan.IllegalRevisionException {
    AtomicInteger numErrors = new AtomicInteger(0);
    Assert.assertEquals(plan.getLastRevisionEpochMillis(), WorkforceStaffing.INITIALIZATION_PROVENANCE_EPOCH_MILLIS);
    Assert.assertEquals(plan.getNumProfiles(), 1);
    plan.reviseWhenNewer(Lists.newArrayList(
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 2,100L),
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 3,500L),
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 4,200L),
        createNewProfileDirective("new_profile", 5,400L, WorkforceProfiles.BASELINE_NAME),
        // NOTE: the second attempt at derivation is NOT judged a duplicate, as the outdated timestamp of first attempt (above) meant it was ignored!
        createNewProfileDirective("new_profile", 6,600L, WorkforceProfiles.BASELINE_NAME),
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 7,800L),
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 8,700L)
    ), failure -> numErrors.incrementAndGet());

    Assert.assertEquals(plan.getLastRevisionEpochMillis(), 800L);
    Assert.assertEquals(plan.getNumProfiles(), 2);
    Assert.assertEquals(numErrors.get(), 0);
    Assert.assertEquals(plan.peepStaffing(WorkforceProfiles.BASELINE_NAME), Optional.of(7), WorkforceProfiles.BASELINE_NAME_RENDERING);
    Assert.assertEquals(plan.peepStaffing("new_profile"), Optional.of(6), "new_profile");
  }

  @Test
  public void reviseWhenNewerSwallowsErrors() throws WorkforcePlan.IllegalRevisionException {
    AtomicInteger numErrors = new AtomicInteger(0);
    plan.reviseWhenNewer(Lists.newArrayList(
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 1,100L),
        // (1) error: `UnrecognizedProfile`
        new ScalingDirective("unknown_profile", 2,250L),
        createNewProfileDirective("new_profile", 3,200L, WorkforceProfiles.BASELINE_NAME),
        // (2) error: `Redefinition`
        createNewProfileDirective("new_profile", 4,450L, WorkforceProfiles.BASELINE_NAME),
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 5,300L),
        // (3) error: `UnknownBasis`
        createNewProfileDirective("other_profile", 6,550L, "never_defined"),
        new ScalingDirective("new_profile", 7,400L),
        // ignored: out-of-order timestamp (not an error... see: `reviseWhenNewerIgnoresOutOfOrderDirectives`)
        new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 8,350L),
        createNewProfileDirective("another", 9,500L, "new_profile")
    ), failure -> numErrors.incrementAndGet());

    Assert.assertEquals(plan.getLastRevisionEpochMillis(), 500L);
    Assert.assertEquals(plan.getNumProfiles(), 3);
    Assert.assertEquals(numErrors.get(), 3);
    Assert.assertEquals(plan.peepStaffing(WorkforceProfiles.BASELINE_NAME), Optional.of(5), WorkforceProfiles.BASELINE_NAME_RENDERING);
    Assert.assertEquals(plan.peepStaffing("new_profile"), Optional.of(7), "new_profile");
    Assert.assertEquals(plan.peepStaffing("another"), Optional.of(9), "another");
  }

  @Test
  public void calcStaffingDeltas() throws WorkforcePlan.IllegalRevisionException {
    plan.revise(createNewProfileDirective("new_profile", 3,10L, WorkforceProfiles.BASELINE_NAME));
    plan.revise(createNewProfileDirective("other_profile", 8,20L, "new_profile"));
    plan.revise(createNewProfileDirective("another", 7,30L, "new_profile"));
    plan.revise(new ScalingDirective("new_profile", 5,40L));
    plan.revise(new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 6,50L));
    plan.revise(new ScalingDirective("another", 4,60L));

    Assert.assertEquals(plan.getLastRevisionEpochMillis(), 60L);
    Assert.assertEquals(plan.getNumProfiles(), 4);
    Assert.assertEquals(plan.peepStaffing(WorkforceProfiles.BASELINE_NAME), Optional.of(6), WorkforceProfiles.BASELINE_NAME_RENDERING);
    Assert.assertEquals(plan.peepStaffing("new_profile"), Optional.of(5), "new_profile");
    Assert.assertEquals(plan.peepStaffing("another"), Optional.of(4), "another");
    Assert.assertEquals(plan.peepStaffing("other_profile"), Optional.of(8), "other_profile");

    WorkforceStaffing referenceStaffing = WorkforceStaffing.initializeStaffing(100, ImmutableMap.of(
        WorkforceProfiles.BASELINE_NAME, 100,
        "new_profile", 1,
        // not initialized - "another"
        "other_profile", 8
    ));
    StaffingDeltas deltas = plan.calcStaffingDeltas(referenceStaffing);
    Assert.assertEquals(deltas.getPerProfileDeltas().size(), 3);
    deltas.getPerProfileDeltas().forEach(delta -> {
      switch (delta.getProfile().getName()) {
        case WorkforceProfiles.BASELINE_NAME:
          Assert.assertEquals(delta.getDelta(), -94);
          Assert.assertEquals(delta.getSetPointProvenanceEpochMillis(), 50L);
          break;
        case "new_profile":
          Assert.assertEquals(delta.getDelta(), 4);
          Assert.assertEquals(delta.getSetPointProvenanceEpochMillis(), 40L);
          break;
        case "another":
          Assert.assertEquals(delta.getDelta(), 4);
          Assert.assertEquals(delta.getSetPointProvenanceEpochMillis(), 60L);
          break;
        case "other_profile": // NOTE: should NOT be present (since delta == 0)!
        default:
          Assert.fail("Unexpected profile: " + delta.getProfile().getName());
      }
    });
  }

  @Test(expectedExceptions = WorkforcePlan.IllegalRevisionException.OutdatedDirective.class)
  public void reviseWithOutdatedDirective() throws WorkforcePlan.IllegalRevisionException {
    plan.revise(new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 7,30000L));
    plan.revise(new ScalingDirective(WorkforceProfiles.BASELINE_NAME, 12,8000L));
  }

  @Test(expectedExceptions = WorkforcePlan.IllegalRevisionException.UnrecognizedProfile.class)
  public void reviseWithUnrecognizedProfileDirective() throws WorkforcePlan.IllegalRevisionException {
    plan.revise(new ScalingDirective("unknown_profile", 7,10000L));
  }

  @Test(expectedExceptions = WorkforcePlan.IllegalRevisionException.Redefinition.class)
  public void reviseWithRedefinitionDirective() throws WorkforcePlan.IllegalRevisionException {
    plan.revise(createNewProfileDirective("new_profile", 5,10000L, WorkforceProfiles.BASELINE_NAME));
    plan.revise(createNewProfileDirective("new_profile", 9,20000L, WorkforceProfiles.BASELINE_NAME));
  }

  @Test(expectedExceptions = WorkforcePlan.IllegalRevisionException.UnknownBasis.class)
  public void reviseWithUnknownBasisDirective() throws WorkforcePlan.IllegalRevisionException {
    plan.revise(createNewProfileDirective("new_profile", 5,10000L, "never_defined"));
  }
}
