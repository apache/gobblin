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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.Data;


/**
 * Collection to map {@link WorkerProfile} names, each to a given set point.  It might be "managed" by a {@link WorkforcePlan}, to reflect
 * desired staffing, or else "unmanaged", where it might represent the current, actual per-worker scaling level.  Those two might then be compared via
 * {@link WorkforcePlan#calcStaffingDeltas(WorkforceStaffing)} to calculate {@link StaffingDeltas} against the "managed" workforce plan.
 */
@ThreadSafe
public class WorkforceStaffing {
  public static final long INITIALIZATION_PROVENANCE_EPOCH_MILLIS = 0L;
  // CAUTION: sentinel value only for use with `StaffingDeltas.ProfileDelta` - NOT for use with `WorkforceStaffing::reviseStaffing`!
  public static final long UNKNOWN_PROVENANCE_EPOCH_MILLIS = -1L;

  /**
   * internal rep. for a set point, with associated provenance timestamp, that will be returned by {@link #calcDeltas(WorkforceStaffing, WorkforceProfiles)},
   * to inform debugging
   */
  @Data
  private static class SetPoint {
    private final int numWorkers;
    private final long provenanceEpochMillis; // for debuggability
  }


  private final Map<String, SetPoint> setPointsByName;

  /** restricted-access ctor: instead use {@link #initialize(int)} */
  private WorkforceStaffing() {
    this.setPointsByName = new ConcurrentHashMap<>();
  }

  /** @return a new instance with `initialBaselineSetPoint` for the "baseline profile's" set point */
  public static WorkforceStaffing initialize(int initialBaselineSetPoint) {
    WorkforceStaffing staffing = new WorkforceStaffing();
    staffing.reviseStaffing(WorkforceProfiles.BASELINE_NAME, initialBaselineSetPoint, INITIALIZATION_PROVENANCE_EPOCH_MILLIS);
    return staffing;
  }

  /** @return [for test init. brevity] a new instance with `initialBaselineSetPoint` for the "baseline profile" set point, plus multiple other set points */
  @VisibleForTesting
  public static WorkforceStaffing initializeStaffing(int initialBaselineSetPoint, Map<String, Integer> initialSetPointsByProfileName) {
    WorkforceStaffing staffing = initialize(initialBaselineSetPoint);
    initialSetPointsByProfileName.forEach((profileName, setPoint) ->
        staffing.reviseStaffing(profileName, setPoint, INITIALIZATION_PROVENANCE_EPOCH_MILLIS)
    );
    return staffing;
  }

  /** @return the staffing set point for the {@link WorkerProfile} named `profileName`, when it exists */
  public Optional<Integer> getStaffing(String profileName) {
    return Optional.ofNullable(setPointsByName.get(profileName)).map(SetPoint::getNumWorkers);
  }

  /** update the staffing set point for the {@link WorkerProfile} named `profileName`, recording `provenanceEpochMillis` as the revision timestamp */
  public void reviseStaffing(String profileName, int setPoint, long provenanceEpochMillis) {
    Preconditions.checkArgument(setPoint >= 0, "set points must be non-negative: '" + profileName + "' had " + setPoint);
    Preconditions.checkArgument(provenanceEpochMillis >= INITIALIZATION_PROVENANCE_EPOCH_MILLIS,
        "provenanceEpochMillis must be non-negative: '" + profileName + "' had " + provenanceEpochMillis);
    setPointsByName.put(profileName, new SetPoint(setPoint, provenanceEpochMillis));
  }

  /** update the staffing set point for the {@link WorkerProfile} named `profileName`, without recording any specific provenance timestamp */
  @VisibleForTesting
  public void updateSetPoint(String profileName, int setPoint) {
    reviseStaffing(profileName, setPoint, INITIALIZATION_PROVENANCE_EPOCH_MILLIS);
  }

  /**
   * @return the {@link StaffingDeltas} between `this` (as "the reference") and `altStaffing`, by using `profiles` to obtain {@link WorkerProfile}s.
   * (A positive {@link StaffingDeltas.ProfileDelta#getDelta()} reflects an increase, while a negative, a decrease.)
   *
   * NOTE: when the same {@link WorkforcePlan} manages both this {@link WorkforceStaffing} and {@link WorkforceProfiles}, then
   * {@link WorkforceProfiles.UnknownProfileException} should NEVER occur.
   */
  public synchronized StaffingDeltas calcDeltas(WorkforceStaffing altStaffing, WorkforceProfiles profiles) {
    Map<String, SetPoint> frozenAltSetPointsByName = new HashMap<>();  // freeze entries for consistency amidst multiple traversals
    altStaffing.setPointsByName.entrySet().forEach(entry -> frozenAltSetPointsByName.put(entry.getKey(), entry.getValue()));
    // not expecting any profile earlier in `reference` to no longer be set... (but defensive coding nonetheless)
    List<StaffingDeltas.ProfileDelta> profileDeltas = frozenAltSetPointsByName.entrySet().stream()
        .filter(entry -> !this.setPointsByName.containsKey(entry.getKey()))
        .map(entry -> new StaffingDeltas.ProfileDelta(profiles.getOrThrow(entry.getKey()), 0 - entry.getValue().getNumWorkers(), UNKNOWN_PROVENANCE_EPOCH_MILLIS))
        .collect(Collectors.toList());
    profileDeltas.addAll(this.setPointsByName.entrySet().stream().map(entry -> {
          Optional<Integer> optEquivAltSetPoint = Optional.ofNullable(frozenAltSetPointsByName.get(entry.getKey())).map(SetPoint::getNumWorkers);
          return new StaffingDeltas.ProfileDelta(
              profiles.getOrThrow(entry.getKey()),
              entry.getValue().getNumWorkers() - optEquivAltSetPoint.orElse(0),
              entry.getValue().getProvenanceEpochMillis());
            }
        ).filter(delta -> delta.isChange())
        .collect(Collectors.toList()));
    return new StaffingDeltas(profileDeltas);
  }
}
