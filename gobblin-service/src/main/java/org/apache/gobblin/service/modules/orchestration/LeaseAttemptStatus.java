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
import lombok.EqualsAndHashCode;
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
   * @return the {@link DagActionStore.DagActionLeaseObject}, containing the dagAction, eventTimeMillis of the event, and boolean
   * indicating if it's a reminder event; {@see MultiActiveLeaseArbiter#tryAcquireLease}
   */
  public DagActionStore.DagActionLeaseObject getConsensusDagActionLeaseObject() {
    return null;
  }

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
  The participant calling this method acquired the lease for the event in question.
  The `consensusLeaseObject` returned by the lease arbitration attempt includes an updated value in the
  `eventTimeMillis` field, which represents the consensus timestamp associated with the lease. For flows that do not
  adopt the consensus `eventTimeMillis` as the flow execution ID, the `dagAction.flowExecutionId` will remain unchanged.
  The consensus `eventTimeMillis` must be tracked for lease completion.
  The time the caller obtained the lease is stored within the`leaseAcquisitionTimestamp` field.
  The `multiActiveLeaseArbiter` reference is used to recordLeaseSuccess for the current LeaseObtainedStatus via the
  completeLease method from a caller without access to the {@link MultiActiveLeaseArbiter}.
  */
  @Data
  // avoid - warning: Generating equals/hashCode implementation but without a call to superclass, even though this class does not extend java.lang.Object
  @EqualsAndHashCode(callSuper=false)
  public static class LeaseObtainedStatus extends LeaseAttemptStatus {
    private final DagActionStore.DagActionLeaseObject consensusDagActionLeaseObject;
    private final long leaseAcquisitionTimestamp;
    private final long minimumLingerDurationMillis;
    @Getter(AccessLevel.NONE)
    private final MultiActiveLeaseArbiter multiActiveLeaseArbiter;

    @Override
    public DagActionStore.DagAction getConsensusDagAction() {
      return consensusDagActionLeaseObject.getDagAction();
    }

    /**
     * Completes the lease referenced by this status object if it has not expired.
     * @return true if able to complete lease, false otherwise.
     * @throws IOException
     */
    public boolean completeLease() throws IOException {
      return multiActiveLeaseArbiter.recordLeaseSuccess(this);
    }

    public long getEventTimeMillis() {
      return consensusDagActionLeaseObject.getEventTimeMillis();
    }
  }

  /*
  This dag action event already has a valid lease owned by another participant.
  See doc for {@link LeaseObtainedStatus} for details about consensusLeaseObject field. Note, that the dag
  action event it corresponds to may be a different and distinct occurrence of the same event.
  `minimumLingerDurationMillis` is the minimum amount of time to wait before this participant should return to check if
  the lease has completed or expired
   */
  @Data
  // avoid - warning: Generating equals/hashCode implementation but without a call to superclass, even though this class does not extend java.lang.Object
  @EqualsAndHashCode(callSuper=false)
  public static class LeasedToAnotherStatus extends LeaseAttemptStatus {
    private final DagActionStore.DagActionLeaseObject consensusDagActionLeaseObject;
    private final long minimumLingerDurationMillis;

    @Override
    public DagActionStore.DagAction getConsensusDagAction() {
      return consensusDagActionLeaseObject.getDagAction();
    }

    public long getEventTimeMillis() {
      return consensusDagActionLeaseObject.getEventTimeMillis();
    }
  }
}
