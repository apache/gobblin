package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;


// TODO: decide btwn making this an abstract class with common functionality or an interface (probably abstract tbh)
@Slf4j
public abstract class GeneralLeaseArbitrationHandler {
  protected Optional<MultiActiveLeaseArbiter> multiActiveLeaseArbiter;
    protected SchedulerService schedulerService;
    protected Optional<DagActionStore> dagActionStore;
    protected MetricContext metricContext;


  private ContextAwareCounter leaseObtainedCount;

  private ContextAwareCounter leasedToAnotherStatusCount;

  private ContextAwareCounter noLongerLeasingStatusCount;
  private ContextAwareCounter jobDoesNotExistInSchedulerCount;
  private ContextAwareCounter failedToSetEventReminderCount;
  private ContextAwareMeter leasesObtainedDueToReminderCount;
  protected ContextAwareMeter failedToRecordLeaseSuccessCount;
  protected ContextAwareMeter recordedLeaseSuccessCount;

  public GeneralLeaseArbitrationHandler(Config config, Optional<MultiActiveLeaseArbiter> leaseDeterminationStore,
      SchedulerService schedulerService, Optional<DagActionStore> dagActionStore) {
    this.multiActiveLeaseArbiter = leaseDeterminationStore;
    this.schedulerService = schedulerService;
    this.dagActionStore = dagActionStore;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass());
    this.leaseObtainedCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASE_OBTAINED_COUNT);
    this.leasedToAnotherStatusCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASED_TO_ANOTHER_COUNT);
    this.noLongerLeasingStatusCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_NO_LONGER_LEASING_COUNT);
    this.jobDoesNotExistInSchedulerCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_JOB_DOES_NOT_EXIST_COUNT);
    this.failedToSetEventReminderCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_FAILED_TO_SET_REMINDER_COUNT);
    this.leasesObtainedDueToReminderCount = this.metricContext.contextAwareMeter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASES_OBTAINED_DUE_TO_REMINDER_COUNT);
    this.failedToRecordLeaseSuccessCount = this.metricContext.contextAwareMeter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_FAILED_TO_RECORD_LEASE_SUCCESS_COUNT);
    this.recordedLeaseSuccessCount = this.metricContext.contextAwareMeter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_RECORDED_LEASE_SUCCESS_COUNT);
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
    public MultiActiveLeaseArbiter.LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis,
        boolean isReminderEvent, boolean skipFlowExecutionIdReplacement)
        throws IOException {
      if (multiActiveLeaseArbiter.isPresent()) {
        MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = multiActiveLeaseArbiter.get().tryAcquireLease(
            flowAction, eventTimeMillis, isReminderEvent, skipFlowExecutionIdReplacement);
        // The flow action contained in the`LeaseAttemptStatus` from the lease arbiter contains an updated flow execution
        // id. From this point onwards, always use the newer version of the flow action to easily track the action through
        // orchestration and execution.
        if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
          if (isReminderEvent) {
            this.leasesObtainedDueToReminderCount.mark();
          }
          this.leaseObtainedCount.inc();
          return leaseAttemptStatus;
        } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus) {
          this.leasedToAnotherStatusCount.inc();
//       TODO:   scheduleReminderForEvent(jobProps,
//              (MultiActiveLeaseArbiter.LeasedToAnotherStatus) leaseAttemptStatus, eventTimeMillis);
          return leaseAttemptStatus;
        } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus) {
          this.noLongerLeasingStatusCount.inc();
          log.debug("Received type of leaseAttemptStatus: [{}, eventTimestamp: {}] ", leaseAttemptStatus.getClass().getName(),
              eventTimeMillis);
          return leaseAttemptStatus;
        }
        throw new RuntimeException(String.format("Received type of leaseAttemptStatus: %s not handled by this method",
            leaseAttemptStatus.getClass().getName()));
      } else {
        throw new RuntimeException(String.format("Multi-active mode is not enabled so dag action event should not be "
            + "handled with this method."));
      }
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
