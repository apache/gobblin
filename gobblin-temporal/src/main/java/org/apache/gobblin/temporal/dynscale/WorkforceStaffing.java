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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import lombok.Data;


public class WorkforceStaffing {
  public static long INITIALIZATION_PROVENANCE_EPOCH_MILLIS = 0L;
  // CAUTION: sentinel value only for use with `StaffingDeltas.ProfileDelta` - NOT for use with `WorkforceStaffing::reviseStaffing`!
  public static long UNKNOWN_PROVENANCE_EPOCH_MILLIS = -1L;

  @Data
  private static class SetPoint {
    private final int point;
    private final long provenanceEpochMillis; // for debuggability
  }


  private final Map<String, SetPoint> setPointByName;

  private WorkforceStaffing() {
    this.setPointByName = new ConcurrentHashMap<>();
  }

  public static WorkforceStaffing initialize(int initialBaselineSetPoint) {
    WorkforceStaffing staffing = new WorkforceStaffing();
    staffing.reviseStaffing(WorkforceProfiles.BASELINE_NAME, initialBaselineSetPoint, INITIALIZATION_PROVENANCE_EPOCH_MILLIS);
    return staffing;
  }

  @VisibleForTesting
  public static WorkforceStaffing initializeStaffing(int initialBaselineSetPoint, Map<String, Integer> initialSetPointsByProfileName) {
    WorkforceStaffing staffing = initialize(initialBaselineSetPoint);
    initialSetPointsByProfileName.forEach((profileName, setPoint) ->
        staffing.reviseStaffing(profileName, setPoint, INITIALIZATION_PROVENANCE_EPOCH_MILLIS)
    );
    return staffing;
  }

  public Optional<Integer> getStaffing(String profileName) {
    return Optional.ofNullable(setPointByName.get(profileName)).map(SetPoint::getPoint);
  }

  public void reviseStaffing(String profileName, int setPoint, long provenanceEpochMillis) {
    Preconditions.checkArgument(setPoint >= 0, "set points must be non-negative: '" + profileName + "' had " + setPoint);
    Preconditions.checkArgument(provenanceEpochMillis >= INITIALIZATION_PROVENANCE_EPOCH_MILLIS,
        "provenanceEpochMillis must be non-negative: '" + profileName + "' had " + provenanceEpochMillis);
    setPointByName.put(profileName, new SetPoint(setPoint, provenanceEpochMillis));
  }

  /**
   * NOTE: so long as the same {@link WorkforcePlan} managed both this {@link WorkforceStaffing} and {@link WorkforceProfiles},
   * {@link WorkforceProfiles.UnknownProfileException} should NOT be possible.
   */
  public synchronized StaffingDeltas calcDeltas(WorkforceStaffing reference, WorkforceProfiles profiles) {
    Map<String, SetPoint> frozenReferenceSetPointsByName = new HashMap<>();  // freeze entries for consistency amidst multiple traversals
    reference.setPointByName.entrySet().forEach(entry -> frozenReferenceSetPointsByName.put(entry.getKey(), entry.getValue()));
    // not expecting any profile earlier in `reference` to no longer be set... (but defensive coding nonetheless)
    List<StaffingDeltas.ProfileDelta> profileDeltas = frozenReferenceSetPointsByName.entrySet().stream()
        .filter(entry -> !this.setPointByName.containsKey(entry.getKey()))
        .map(entry -> new StaffingDeltas.ProfileDelta(profiles.getOrThrow(entry.getKey()), 0 - entry.getValue().getPoint(), UNKNOWN_PROVENANCE_EPOCH_MILLIS))
        .collect(Collectors.toList());
    profileDeltas.addAll(this.setPointByName.entrySet().stream().map(entry -> {
          Optional<Integer> optEquivReferenceSetPoint = Optional.ofNullable(frozenReferenceSetPointsByName.get(entry.getKey())).map(SetPoint::getPoint);
          return new StaffingDeltas.ProfileDelta(
              profiles.getOrThrow(entry.getKey()),
              entry.getValue().getPoint() - optEquivReferenceSetPoint.orElse(0),
              entry.getValue().getProvenanceEpochMillis());
            }
        ).filter(delta -> !delta.isUnchanged())
        .collect(Collectors.toList()));
    return new StaffingDeltas(profileDeltas);
  }
}
