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

package org.apache.gobblin.temporal.loadgen.dynamic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.gobblin.yarn.GobblinYarnConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.ProfileDerivation;
import org.apache.gobblin.temporal.dynamic.ProfileOverlay;
import org.apache.gobblin.temporal.dynamic.ScalingDirective;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.WorkforceProfiles;


/**
 * A dummy implementation of {@link ScalingDirectiveSource} that returns a fixed set of {@link ScalingDirective}s.
 */
public class DummyScalingDirectiveSource implements ScalingDirectiveSource {
  private final AtomicInteger numInvocations = new AtomicInteger(0);
  private final Optional<ProfileDerivation> derivedFromBaseline;
  public DummyScalingDirectiveSource() {
    this.derivedFromBaseline = Optional.of(new ProfileDerivation(WorkforceProfiles.BASELINE_NAME,
        new ProfileOverlay.Adding(
            new ProfileOverlay.KVPair(GobblinYarnConfigurationKeys.CONTAINER_MEMORY_MBS_KEY, "2048"),
            new ProfileOverlay.KVPair(GobblinYarnConfigurationKeys.CONTAINER_CORES_KEY, "2")
        )
    ));
  }

  /**
   * @return {@link ScalingDirective}s - an impl. may choose to return all known directives or to give only newer
   * directives than previously returned
   */
  @Override
  public List<ScalingDirective> getScalingDirectives() {
    // Note - profile should exist already pr is derived from other profile
    if (this.numInvocations.get() == 0) {
      this.numInvocations.getAndIncrement();
      // here we are returning two new profile with initial container counts and these should be launched
      long currentTime = System.currentTimeMillis();
      // both profiles should have different timestampEpochMillis so that both are processed otherwise
      // }org.apache.gobblin.temporal.dynamic.WorkforcePlan$IllegalRevisionException$OutOfOrderDirective can occur
      return Arrays.asList(
          new ScalingDirective("firstProfile", 3, currentTime, this.derivedFromBaseline),
          new ScalingDirective("secondProfile", 2, currentTime + 1, this.derivedFromBaseline)
      );
    } else if (this.numInvocations.get() == 1) {
      this.numInvocations.getAndIncrement();
      // here we are increasing containers to 5 for firstProfile and 3 for secondProfile so that 2 new extra containers
      // should be launched for firstProfile and 1 new extra container for secondProfile
      long currentTime = System.currentTimeMillis();
      return Arrays.asList(
          new ScalingDirective("firstProfile", 5, currentTime),
          new ScalingDirective("secondProfile", 3, currentTime + 1)
      );
    } else if (this.numInvocations.get() == 2) {
      this.numInvocations.getAndIncrement();
      // the count is same as previous invocation so no new containers should be launched
      long currentTime = System.currentTimeMillis();
      return Arrays.asList(
          new ScalingDirective("firstProfile", 5, currentTime),
          new ScalingDirective("secondProfile", 3, currentTime + 1)
      );
    }
    return new ArrayList<>();
  }
}
