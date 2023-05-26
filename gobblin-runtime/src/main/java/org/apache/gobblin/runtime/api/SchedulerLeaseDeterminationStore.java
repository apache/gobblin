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

package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Interface defines the two basic actions required for lease determination for each FlowActionType event for a flow.
 * It is used by the {@link SchedulerLeaseAlgoHandler} to allow multiple scheduler's on different hosts to determine
 * which scheduler is tasked with ensuring the FlowAction is taken for the trigger.
 */
public interface SchedulerLeaseDeterminationStore {
  static final Logger LOG = LoggerFactory.getLogger(SchedulerLeaseDeterminationStore.class);

  // Enum is used to reason about the three possible scenarios that can result from an attempt to obtain a lease for a
  // particular trigger event of a flow
  enum LeaseAttemptStatus {
    LEASE_OBTAINED,
    PREVIOUS_LEASE_EXPIRED,
    PREVIOUS_LEASE_VALID
  }

  // Action to take on a particular flow
  enum FlowActionType {
    LAUNCH,
    RETRY,
    CANCEL,
    NEXT_HOP
  }

  /**
   * This method attempts to insert an entry into store for a particular flow's trigger event if one does not already
   * exist in the store for the same trigger event. Regardless of the outcome it also reads the pursuant timestamp of
   * the entry for that trigger event (it could have pre-existed in the table or been newly added by the previous
   * write). Based on the transaction results, it will return @LeaseAttemptStatus to determine the next action.
   * @param flowGroup
   * @param flowName
   * @param flowExecutionId
   * @param triggerTimeMillis is the time this flow is supposed to be launched
   * @return LeaseAttemptStatus
   * @throws IOException
   */
  LeaseAttemptStatus attemptInsertAndGetPursuantTimestamp(String flowGroup, String flowName,
      String flowExecutionId, FlowActionType flowActionType, long triggerTimeMillis) throws IOException;

  /**
   * This method is used by `attemptInsertAndGetPursuantTimestamp` above to indicate the host has successfully completed
   * actions necessary to confirm the lease of a flow trigger event.
   * @return true if successfully updated, indicating no further actions need to be taken regarding this event.
   */
  boolean updatePursuantTimestamp(String flowGroup, String flowName, String flowExecutionId,
      FlowActionType flowActionType, Timestamp triggerTimestamp) throws IOException;
}
