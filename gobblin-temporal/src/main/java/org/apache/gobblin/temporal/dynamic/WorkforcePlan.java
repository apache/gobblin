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

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stateful class to maintain the dynamically scalable workforce plan for {@link WorkerProfile}s with a {@link WorkforceStaffing} set point
 * for each.  The plan evolves through incremental revision by {@link ScalingDirective}s, while {@link #calcStaffingDeltas(WorkforceStaffing)}
 * reports {@link StaffingDeltas} between the current plan and another alternative (e.g. current level of) {@link WorkforceStaffing}.
 */
@Slf4j
@ThreadSafe
public class WorkforcePlan {

  /** Common baseclass for illegal plan revision */
  public static class IllegalRevisionException extends Exception {
    @Getter private final ScalingDirective directive;
    private IllegalRevisionException(ScalingDirective directive, String msg) {
      super(msg);
      this.directive = directive;
    }

    /** Illegal revision: directive arrived out of {@link ScalingDirective#getTimestampEpochMillis()} order */
    public static class OutOfOrderDirective extends IllegalRevisionException {
      protected OutOfOrderDirective(ScalingDirective directive, long lastRevisionEpochMillis) {
        super(directive, "directive for profile '" + directive.renderName() + "' precedes last revision at "
            + lastRevisionEpochMillis + ": " + directive);
      }
    }

    /** Illegal revision: redefinition of a known worker profile */
    public static class Redefinition extends IllegalRevisionException {
      protected Redefinition(ScalingDirective directive, ProfileDerivation proposedDerivation) {
        super(directive, "profile '" + directive.renderName() + "' already exists, so may not be redefined on the basis of '"
            + proposedDerivation.renderName() + "': " + directive);
      }
    }

    /** Illegal revision: worker profile evolution from an unknown basis profile */
    public static class UnknownBasis extends IllegalRevisionException {
      protected UnknownBasis(ScalingDirective directive, ProfileDerivation.UnknownBasisException ube) {
        super(directive, "profile '" + directive.renderName() + "' may not be defined on the basis of an unknown profile '"
            + WorkforceProfiles.renderName(ube.getName()) + "': " + directive);
      }
    }

    /** Illegal revision: set point for an unknown worker profile */
    public static class UnrecognizedProfile extends IllegalRevisionException {
      protected UnrecognizedProfile(ScalingDirective directive) {
        super(directive, "unrecognized profile reference '" + directive.renderName() + "': " + directive);
      }
    }
  }

  private final WorkforceProfiles profiles;
  private final WorkforceStaffing staffing;
  @Getter private volatile long lastRevisionEpochMillis;

  /** create new plan with the initial, baseline worker profile using `baselineConfig` at `initialSetPoint` */
  public WorkforcePlan(Config baselineConfig, int initialSetPoint) {
    this.profiles = WorkforceProfiles.withBaseline(baselineConfig);
    this.staffing = WorkforceStaffing.initialize(0);
    // Initial containers use the global baseline profile
    this.staffing.reviseStaffing(
        WorkforceProfiles.BASELINE_NAME,
        initialSetPoint,
        0
    );
    this.lastRevisionEpochMillis = 0;
  }

  /** @return how many worker profiles known to the plan, including the baseline */
  public int getNumProfiles() {
    return profiles.size();
  }

  /** revise the plan with a new {@link ScalingDirective} or throw {@link IllegalRevisionException} */
  public synchronized void revise(ScalingDirective directive) throws IllegalRevisionException {
    String name = directive.getProfileName();
    if (this.lastRevisionEpochMillis >= directive.getTimestampEpochMillis()) {
      throw new IllegalRevisionException.OutOfOrderDirective(directive, this.lastRevisionEpochMillis);
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
      // TODO - make idempotent, since any retry attempt following failure between `addProfile` and `reviseStaffing` would thereafter fail with
      // `IllegalRevisionException.Redefinition`, despite us wishing to adjust the set point for that new profile defined just before the failure.
      //   - how to ensure the profile def is the same / unchanged?  (e.g. compare full profile `Config` equality)?
      // NOTE: the current outcome would be a profile defined in `WorkforceProfiles` with no set point in `WorkforceStaffing`.  fortunately,
      // that would NOT lead to `calcStaffingDeltas` throwing {@link WorkforceProfiles.UnknownProfileException}!  The out-of-band (manual)
      // workaround/repair would be revision by another, later directive that provides the set point for that profile (WITHOUT providing the definition)

      this.staffing.reviseStaffing(name, directive.getSetPoint(), directive.getTimestampEpochMillis());
      this.lastRevisionEpochMillis = directive.getTimestampEpochMillis();
    }
  }

  /**
   * Performs atomic bulk revision while enforcing `directives` ordering in accord with {@link ScalingDirective#getTimestampEpochMillis()}
   *
   * This version catches {@link IllegalRevisionException}, to log a warning message before continuing to process subsequent directives.
   */
  public synchronized void reviseWhenNewer(List<ScalingDirective> directives) {
    reviseWhenNewer(directives, ire -> { log.warn("Failure: ", ire); });
  }

  /**
   * Performs atomic bulk revision while enforcing `directives` ordering in accord with {@link ScalingDirective#getTimestampEpochMillis()}
   *
   * This version catches {@link IllegalRevisionException}, to call `illegalRevisionHandler` before continuing to process subsequent directives.
   */
  public synchronized void reviseWhenNewer(List<ScalingDirective> directives, Consumer<IllegalRevisionException> illegalRevisionHandler) {
    directives.stream().sequential()
        // filter, to avoid `OutOfOrderDirective` exceptions that would clutter the log - especially since `reviseWhenNewer` suggests graceful handling
        .filter(directive -> this.lastRevisionEpochMillis < directive.getTimestampEpochMillis())
        .forEach(directive -> {
      try {
        revise(directive);
      } catch (IllegalRevisionException ire) {
        illegalRevisionHandler.accept(ire);
      }
    });
  }

  /** @return diff of {@link StaffingDeltas} between this, current {@link WorkforcePlan} and some `altStaffing` (e.g. current) {@link WorkforceStaffing} */
  public synchronized StaffingDeltas calcStaffingDeltas(WorkforceStaffing altStaffing) {
    return staffing.calcDeltas(altStaffing, profiles);
  }

  /** @return [for testing/debugging] the current staffing set point for the {@link WorkerProfile} named `profileName`, when it exists */
  @VisibleForTesting
  Optional<Integer> peepStaffing(String profileName) {
    return staffing.getStaffing(profileName);
  }

  /** @return [for testing/debugging] the {@link WorkerProfile} named `profileName` or throws {@link WorkforceProfiles.UnknownProfileException} */
  @VisibleForTesting
  WorkerProfile peepProfile(String profileName) throws WorkforceProfiles.UnknownProfileException {
    return profiles.getOrThrow(profileName);
  }

  /** @return [for testing/debugging] the baseline {@link WorkerProfile} - it should NEVER throw {@link WorkforceProfiles.UnknownProfileException} */
  @VisibleForTesting
  WorkerProfile peepBaselineProfile() throws WorkforceProfiles.UnknownProfileException {
    return profiles.getOrThrow(WorkforceProfiles.BASELINE_NAME);
  }

}
