package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  public LeaseAttemptStatus attemptInsertAndGetPursuantTimestamp(String flowGroup, String flowName,
      String flowExecutionId, FlowActionType flowActionType, long triggerTimeMillis) throws IOException;

  public boolean updatePursuantTimestamp(String flowGroup, String flowName, String flowExecutionId,
      FlowActionType flowActionType, Timestamp triggerTimestamp) throws IOException;
}
