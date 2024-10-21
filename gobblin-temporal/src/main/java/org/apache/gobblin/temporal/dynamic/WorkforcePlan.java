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

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import com.typesafe.config.Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class WorkforcePlan {

  public static class IllegalRevisionException extends Exception {
    @Getter private final ScalingDirective directive;
    private IllegalRevisionException(ScalingDirective directive, String msg) {
      super(msg);
      this.directive = directive;
    }

    public static class OutdatedDirective extends IllegalRevisionException {
      protected OutdatedDirective(ScalingDirective directive, long lastRevisionEpochMillis) {
        super(directive, "directive for profile '" + directive.renderName() + "' precedes last revision at "
            + lastRevisionEpochMillis + ": " + directive);
      }
    }

    public static class Redefinition extends IllegalRevisionException {
      protected Redefinition(ScalingDirective directive, ProfileDerivation proposedDerivation) {
        super(directive, "profile '" + directive.renderName() + "' already exists, so may not be redefined on the basis of '"
            + proposedDerivation.renderName() + "': " + directive);
      }
    }

    public static class UnrecognizedProfile extends IllegalRevisionException {
      protected UnrecognizedProfile(ScalingDirective directive) {
        super(directive, "unrecognized profile reference '" + directive.renderName() + "': " + directive);
      }
    }

    public static class UnknownBasis extends IllegalRevisionException {
      protected UnknownBasis(ScalingDirective directive, ProfileDerivation.UnknownBasisException ube) {
        super(directive, "profile '" + directive.renderName() + "' may not be defined on the basis of an unknown profile '"
            + WorkforceProfiles.renderName(ube.getName()) + "': " + directive);
      }
    }
  }

  private final WorkforceProfiles profiles;
  private final WorkforceStaffing staffing;
  @Getter private volatile long lastRevisionEpochMillis;

  public WorkforcePlan(Config baselineConfig, int initialSetPoint) {
    this.profiles = WorkforceProfiles.withBaseline(baselineConfig);
    this.staffing = WorkforceStaffing.initialize(initialSetPoint);
    this.lastRevisionEpochMillis = 0;
  }

  public int getNumProfiles() {
    return profiles.size();
  }

  public synchronized void revise(ScalingDirective directive) throws IllegalRevisionException {
    String name = directive.getProfileName();
    if (this.lastRevisionEpochMillis >= directive.getTimestampEpochMillis()) {
      throw new IllegalRevisionException.OutdatedDirective(directive, this.lastRevisionEpochMillis);
    };
    Optional<WorkerProfile> optExistingProfile = profiles.apply(name);
    Optional<ProfileDerivation> optDerivation = directive.getOptDerivedFrom();
    if (optExistingProfile.isPresent() && optDerivation.isPresent()) {
      throw new IllegalRevisionException.Redefinition(directive, optDerivation.get());
    } else if (!optExistingProfile.isPresent() && !optDerivation.isPresent()) {
      throw new IllegalRevisionException.UnrecognizedProfile(directive);
    } else {  // [exclusive-or: either, but not both present]
      if (optDerivation.isPresent()) { // define a new profile on the basis of another
        try {
          this.profiles.addProfile(new WorkerProfile(name, optDerivation.get().formulateConfig(this.profiles)));
        } catch (ProfileDerivation.UnknownBasisException ube) {
          throw new IllegalRevisionException.UnknownBasis(directive, ube);
        }
      }
      // TODO - make idempotent, as re-attempts after failure between `addProfile` and `reviseStaffing` would fail with `IllegalRevisionException.Redefinition`
      // adjust the set-point now that either a new profile is defined OR the profile already existed
      this.staffing.reviseStaffing(name, directive.getSetPoint(), directive.getTimestampEpochMillis());
      this.lastRevisionEpochMillis = directive.getTimestampEpochMillis();
    }
  }

  /** atomic bulk revision
   *
   * !!!!requires sorted order of directives by timestamp!!!!
   *
   */
  public synchronized void reviseWhenNewer(List<ScalingDirective> directives) {
    reviseWhenNewer(directives, ire -> { log.warn("Failure: ", ire); });
  }

  public synchronized void reviseWhenNewer(List<ScalingDirective> directives, Consumer<IllegalRevisionException> illegalRevisionHandler) {
    directives.stream().sequential()
        .filter(directive -> directive.getTimestampEpochMillis() > this.lastRevisionEpochMillis)
        .forEach(directive -> {
      try {
        revise(directive);
      } catch (IllegalRevisionException ire) {
        System.err.println("uh oh it's: " + ire);
        illegalRevisionHandler.accept(ire);
      }
    });
  }

  /** @returns diff of {@link StaffingDeltas} of this, current {@link WorkforcePlan} against some `reference` {@link WorkforceStaffing} */
  public synchronized StaffingDeltas calcStaffingDeltas(WorkforceStaffing reference) {
    return staffing.calcDeltas(reference, profiles);
  }

  @VisibleForTesting
  Optional<Integer> peepStaffing(String profileName) {
    return staffing.getStaffing(profileName);
  }

  @VisibleForTesting
  WorkerProfile peepProfile(String profileName) throws WorkforceProfiles.UnknownProfileException {
    return profiles.getOrThrow(profileName);
  }

  @VisibleForTesting
  WorkerProfile peepBaselineProfile() throws WorkforceProfiles.UnknownProfileException {
    return profiles.getOrThrow(WorkforceProfiles.BASELINE_NAME);
  }
}
