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
import lombok.Data;
import lombok.RequiredArgsConstructor;


@Data
@RequiredArgsConstructor
public class ScalingDirective {
  private final String profileName;
  private final int setPoint;
  private final long timestampEpochMillis;
  private final Optional<ProfileDerivation> optDerivedFrom;

  public ScalingDirective(String profileName, int setPoint, long timestampEpochMillis) {
    this(profileName, setPoint, timestampEpochMillis, Optional.empty());
  }

  public ScalingDirective(String profileName, int setPoint, long timestampEpochMillis, String basisProfileName, ProfileOverlay overlay) {
    this(profileName, setPoint, timestampEpochMillis, Optional.of(new ProfileDerivation(basisProfileName, overlay)));
  }

  public String renderName() {
    return WorkforceProfiles.renderName(this.profileName);
  }
}
