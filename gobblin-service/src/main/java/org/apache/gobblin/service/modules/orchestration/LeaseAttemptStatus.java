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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;


/**
 * Hierarchy to convey the specific outcome of attempted lease acquisition via the {@link MultiActiveLeaseArbiter},
 * with each derived type carrying outcome-specific status info.
 *
 * IMPL. NOTE: {@link LeaseAttemptStatus#getConsensusDagAction} and {@link LeaseAttemptStatus#getMinimumLingerDurationMillis}
 * intended for `@Override`.
 */
public abstract class LeaseAttemptStatus {
  /**
   * @return the {@link DagActionStore.DagAction}, which may now have an updated flowExecutionId that MUST henceforth be
   * used; {@see MultiActiveLeaseArbiter#tryAcquireLease}
   */
  public DagActionStore.DagAction getConsensusDagAction() {
    return null;
  }

  public long getMinimumLingerDurationMillis() {
    return 0;
  }

  /*
   * This LeaseAttemptStatus tells the caller that work for which lease was requested is completed and thus lease is no
   * longer required. There is also no need to set reminder events.
   */
  public static class NoLongerLeasingStatus extends LeaseAttemptStatus {}

  /*
  The participant calling this method acquired the lease for the event in question. `Dag action`'s flow execution id
  is the timestamp associated with the lease and the time the caller obtained the lease is stored within the
  `leaseAcquisitionTimestamp` field. The `multiActiveLeaseArbiter` reference is used to recordLeaseSuccess for the
  current LeaseObtainedStatus via the completeLease method from a caller without access to the {@link MultiActiveLeaseArbiter}.
  */
  @Data
  public static class LeaseObtainedStatus extends LeaseAttemptStatus {
    private final DagActionStore.DagAction consensusDagAction;
    private final long leaseAcquisitionTimestamp;
    private final long minimumLingerDurationMillis;
    @Getter(AccessLevel.NONE)
    private final MultiActiveLeaseArbiter multiActiveLeaseArbiter;

    /**
     * @return event time in millis since epoch for the event of this lease acquisition
     */
    public long getEventTimeMillis() {
      return Long.parseLong(consensusDagAction.getFlowExecutionId());
    }

    /**
     * Completes the lease referenced by this status object if it has not expired.
     * @return true if able to complete lease, false otherwise.
     * @throws IOException
     */
    public boolean completeLease() throws IOException {
      return multiActiveLeaseArbiter.recordLeaseSuccess(this);
    }
  }

  /*
  This dag action event already has a valid lease owned by another participant.
  `Dag action`'s flow execution id is the timestamp the lease is associated with, however the dag action event it
  corresponds to may be a different and distinct occurrence of the same event.
  `minimumLingerDurationMillis` is the minimum amount of time to wait before this participant should return to check if
  the lease has completed or expired
   */
  @Data
  public static class LeasedToAnotherStatus extends LeaseAttemptStatus {
    private final DagActionStore.DagAction consensusDagAction;
    private final long minimumLingerDurationMillis;

    /**
     * Returns event time in millis since epoch for the event whose lease was obtained by another participant.
     * @return
     */
    public long getEventTimeMillis() {
      return Long.parseLong(consensusDagAction.getFlowExecutionId());
    }
  }
}
