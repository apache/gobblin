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

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Core abstraction to model scaling adjustment: a directive originates at a specific moment in time to provide a set point for a given worker profile.
 * The set point is the number of instances presently desired for that profile.  When naming a heretofore unknown worker profile, the directive MUST also
 * define that new profile through a {@link ProfileDerivation} referencing a known profile.  Once defined, a worker profile MUST NOT be redefined.
 */
@Data
@Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
/*
 * NOTE: due to type erasure, neither alternative approach works when returning a collection of `ScalingDirective`s (only when a direct `ScalingDirective`)
 *   see: https://github.com/FasterXML/jackson-databind/issues/336
 * instead, `@JsonProperty("this")` clarifies the class name in serialized form
 *    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "$this")
 *    @JsonTypeInfo(include=JsonTypeInfo.As.WRAPPER_OBJECT, use=JsonTypeInfo.Id.NAME)
 */
@JsonPropertyOrder({ "this", "profileName", "setPoint", "optDerivedFrom", "timestampEpochMillis" }) // improve readability (e.g. in the temporal UI)
@JsonIgnoreProperties(ignoreUnknown = true) /* necessary since no `setThis` setter (to act as inverse of `supplyJsonClassSimpleName`), so to avoid:
 * com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException: Unrecognized field \"this\" (class ...dynamic.ScalingDirective), not marked as ignorable
 */
public class ScalingDirective {
  @NonNull private String profileName;
  // NOTE: `@NonNull` to include field in `@RequiredArgsConstructor`, despite - "warning: @NonNull is meaningless on a primitive... @RequiredArgsConstructor"
  @NonNull private int setPoint;
  @NonNull private long timestampEpochMillis;
  @NonNull private Optional<ProfileDerivation> optDerivedFrom;

  /** purely for observability: announce class to clarify serialized form */
  @JsonProperty("this")
  public String supplyJsonClassSimpleName() {
    return this.getClass().getSimpleName();
  }


  /** Create a set-point-only directive (for a known profile, with no {@link ProfileDerivation}) */
  public ScalingDirective(String profileName, int setPoint, long timestampEpochMillis) {
    this(profileName, setPoint, timestampEpochMillis, Optional.empty());
  }

  public ScalingDirective(String profileName, int setPoint, long timestampEpochMillis, String basisProfileName, ProfileOverlay overlay) {
    this(profileName, setPoint, timestampEpochMillis, Optional.of(new ProfileDerivation(basisProfileName, overlay)));
  }

  /** @return a new `ScalingDirective`, otherwise unchanged, but with {@link ScalingDirective#setPoint} replaced by `newSetPoint` */
  public ScalingDirective updateSetPoint(int newSetPoint) {
    return new ScalingDirective(this.profileName, newSetPoint, this.timestampEpochMillis, this.optDerivedFrom);
  }

  /** @return the canonical *display* name (for {@link #getProfileName()}) for tracing/debugging */
  public String renderName() {
    return WorkforceProfiles.renderName(this.profileName);
  }
}
