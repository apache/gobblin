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

package org.apache.gobblin.temporal.ddm.work;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;


/**
 * Duration for whatever work to complete, with a permitted overage to indicate firm-ness/soft-ness.
 * Values are in minutes, befitting the granularity of inevitable companion activities, like:
 *   - network operations - opening connections, I/O, retries
 *   - starting/scaling workers
 */
@Data
@Setter(AccessLevel.NONE) // NOTE: non-`final` members solely to enable deserialization
@NoArgsConstructor // IMPORTANT: for jackson (de)serialization
@RequiredArgsConstructor
public class TimeBudget {
  // NOTE: `@NonNull` to include field in `@RequiredArgsConstructor`, despite - "warning: @NonNull is meaningless on a primitive... @RequiredArgsConstructor"
  @NonNull private long maxDurationDesiredMinutes;
  @NonNull private long permittedOverageMinutes;

  /** construct w/ {@link #permittedOverageMinutes} expressed as a percentage of {@link #maxDurationDesiredMinutes} */
  public static TimeBudget withOveragePercentage(long maxDurationDesiredMinutes, double permittedOveragePercentage) {
    return new TimeBudget(maxDurationDesiredMinutes, (long) (maxDurationDesiredMinutes * permittedOveragePercentage));
  }
}
