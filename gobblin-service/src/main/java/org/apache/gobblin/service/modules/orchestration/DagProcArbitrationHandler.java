package org.apache.gobblin.service.modules.orchestration;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.scheduler.SchedulerService;


// TODO: what additional methods are needed here?
public class DagProcArbitrationHandler extends GeneralLeaseArbitrationHandler {

  public DagProcArbitrationHandler(Config config, Optional<MultiActiveLeaseArbiter> leaseDeterminationStore,
      SchedulerService schedulerService, Optional<DagActionStore> dagActionStore) {
    super(config, leaseDeterminationStore, schedulerService, dagActionStore);
  }

  /**
   * This method is used by the multi-active scheduler and multi-active execution classes (DagTaskStream) to attempt a
   * lease for a particular job event and return the status of the attempt.
   * @param flowAction
   * @param eventTimeMillis
   * @param isReminderEvent
   * @param skipFlowExecutionIdReplacement
   * @return
   */
  @Override
  public MultiActiveLeaseArbiter.LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis,
      boolean isReminderEvent, boolean skipFlowExecutionIdReplacement) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * To be called by a lease owner of a dagAction to mark the action as done.
   * @param leaseStatus
   */
  public void recordSuccessfulCompletion(MultiActiveLeaseArbiter.LeaseObtainedStatus leaseStatus) {
    // call MA.recordLeaseSuccess();
  }

  /**
   * This method is used by the callers of the lease arbitration attempts over a dag action event to schedule a
   * self-reminder to check on the other participant's progress to finish acting on a dag action after the time the
   * lease should expire.
   * @param leaseStatus
   * @param triggerEventTimeMillis
   */
  public void scheduleReminderForEvent(MultiActiveLeaseArbiter.LeasedToAnotherStatus leaseStatus, long triggerEventTimeMillis) {
    throw new UnsupportedOperationException("Not supported");
  }
}
