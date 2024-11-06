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
import lombok.Data;


/** Staffing set point {@link ProfileDelta}s for multiple {@link WorkerProfile}s */
@Data
public class StaffingDeltas {
  /**
   * Difference for a {@link WorkerProfile}'s staffing set point (e.g. between desired and current levels).  Positive `delta` reflects increase,
   * while negative, a decrease.
   */
  @Data
  public static class ProfileDelta {
    private final WorkerProfile profile;
    private final int delta;
    private final long setPointProvenanceEpochMillis;

    /** @return whether {@link #getDelta()} is non-zero */
    public boolean isChange() {
      return delta != 0;
    }
  }


  private final List<ProfileDelta> perProfileDeltas;
}
