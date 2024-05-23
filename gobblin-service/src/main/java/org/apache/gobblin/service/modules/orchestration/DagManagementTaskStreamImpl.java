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
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.quartz.SchedulerException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import javax.inject.Named;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.task.EnforceFlowFinishDeadlineDagTask;
import org.apache.gobblin.service.modules.orchestration.task.EnforceJobStartDeadlineDagTask;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ReevaluateDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ResumeDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.util.ConfigUtils;


/**
 * DagManagementTaskStreamImpl implements {@link DagManagement} and {@link DagTaskStream}. It accepts
 * {@link org.apache.gobblin.service.modules.orchestration.DagActionStore.DagAction}s and iteratively provides
 * {@link DagTask}.
 *
 * It uses {@link MultiActiveLeaseArbiter} to coordinate multiple hosts with execution components enabled in
 * multi-active execution mode to respond to flow action events by attempting ownership over a flow action event at a
 * given event time. Only events that the current instance acquires a lease for are selected by
 * {@link DagManagementTaskStreamImpl#next()}. If the status of the lease ownership attempt is anything other than an
 * indication the lease has been completed
 * ({@link LeaseAttemptStatus}) then the {@link MultiActiveLeaseArbiter#tryAcquireLease} method will set a reminder for
 * the flow action using {@link DagActionReminderScheduler} to reattempt the lease after the current leaseholder's grant
 * would have expired. The {@link DagActionReminderScheduler} is used in the non multi-active execution configuration as
 * well to utilize reminders for a single {@link DagManagementTaskStreamImpl} case as well.
 * Note that if multi-active execution is NOT enabled, then all flow action events are selected by
 * {@link DagManagementTaskStreamImpl#next()} by virtue of having no other contenders for the lease at the time
 * {@link MultiActiveLeaseArbiter#tryAcquireLease} is called.
 */
@Slf4j
@Singleton
@Data
public class DagManagementTaskStreamImpl implements DagManagement, DagTaskStream {
  private final Config config;
  @Getter private final EventSubmitter eventSubmitter;

  @Inject(optional=true)
  protected Optional<DagActionStore> dagActionStore;
  protected MultiActiveLeaseArbiter dagActionProcessingLeaseArbiter;
  protected Optional<DagActionReminderScheduler> dagActionReminderScheduler;
  private final boolean isMultiActiveExecutionEnabled;
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;
  private final BlockingQueue<DagActionStore.DagAction> dagActionQueue = new LinkedBlockingQueue<>();
  private final DagManagementStateStore dagManagementStateStore;

  @Inject
  public DagManagementTaskStreamImpl(Config config, Optional<DagActionStore> dagActionStore,
      @Named(ConfigurationKeys.PROCESSING_LEASE_ARBITER_NAME) MultiActiveLeaseArbiter dagActionProcessingLeaseArbiter,
      Optional<DagActionReminderScheduler> dagActionReminderScheduler,
      @Named(InjectionNames.MULTI_ACTIVE_EXECUTION_ENABLED) boolean isMultiActiveExecutionEnabled,
      DagManagementStateStore dagManagementStateStore) {
    this.config = config;
    if (!dagActionStore.isPresent()) {
      /* DagActionStore is optional because there are other configurations that do not require it and it's initialized
      in {@link GobblinServiceGuiceModule} which handles all possible configurations */
      throw new RuntimeException("DagProcessingEngine should not be enabled without dagActionStore enabled.");
    }
    if (!dagActionReminderScheduler.isPresent()) {
      throw new RuntimeException(String.format("DagProcessingEngine requires %s to be instantiated.",
          DagActionReminderScheduler.class.getSimpleName()));
    }
    this.dagActionStore = dagActionStore;
    this.dagActionProcessingLeaseArbiter = dagActionProcessingLeaseArbiter;
    this.dagActionReminderScheduler = dagActionReminderScheduler;
    this.isMultiActiveExecutionEnabled = isMultiActiveExecutionEnabled;
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
    this.dagManagementStateStore = dagManagementStateStore;
  }

  @Override
  public synchronized void addDagAction(DagActionStore.DagAction dagAction) {
    // TODO: Used to track missing dag issue, remove later as needed
    log.info("Add dagAction {}", dagAction);

    if (!this.dagActionQueue.offer(dagAction)) {
      throw new RuntimeException("Could not add dag action " + dagAction + " to the queue");
    }
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public DagTask next() {
      while (true) {
        try {
          DagActionStore.DagAction dagAction = this.dagActionQueue.take();
          /* Create triggers for original (non-reminder) dag actions of type ENFORCE_JOB_START_DEADLINE and ENFORCE_FLOW_FINISH_DEADLINE.
             Reminder triggers are used to inform hosts once the job start deadline and flow finish deadline are passed;
             then only is lease arbitration done to enforce the deadline violation and fail the job or flow if needed */
          if (!dagAction.isReminder() && dagAction.dagActionType == DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE) {
            createJobStartDeadlineTrigger(dagAction);
          } else if (!dagAction.isReminder() && dagAction.dagActionType == DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE) {
            createFlowFinishDeadlineTrigger(dagAction);
          } else if (!dagAction.isReminder
              || dagAction.dagActionType == DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE
              || dagAction.dagActionType == DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE) {
            // todo - fix bug of a reminder event getting a lease even when the first attempt succeeded.
            // for now, avoid processing reminder events if they are not for deadline dag actions
            LeaseAttemptStatus leaseAttemptStatus = retrieveLeaseStatus(dagAction);
            if (leaseAttemptStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus) {
              return createDagTask(dagAction, (LeaseAttemptStatus.LeaseObtainedStatus) leaseAttemptStatus);
            }
          }
        } catch (Exception e) {
          //TODO: need to handle exceptions gracefully
          log.error("Exception getting DagAction from the queue / creating DagTask", e);
        }
      }
  }

  private void createJobStartDeadlineTrigger(DagActionStore.DagAction dagAction) throws SchedulerException, IOException {
    long timeOutForJobStart = DagManagerUtils.getJobStartSla(this.dagManagementStateStore.getDag(
        dagAction.getDagId()).get().getNodes().get(0), DagProcessingEngine.getDefaultJobStartSlaTimeMillis());
    // todo - this timestamp is just an approximation, the real job submission has happened in past, and that is when a
    // ENFORCE_JOB_START_DEADLINE dag action was created; we are just processing that dag action here
    long jobSubmissionTime = System.currentTimeMillis();
    long reminderDuration = jobSubmissionTime + timeOutForJobStart - System.currentTimeMillis();

    dagActionReminderScheduler.get().scheduleReminder(dagAction, reminderDuration);
  }

  private void createFlowFinishDeadlineTrigger(DagActionStore.DagAction dagAction) throws SchedulerException, IOException {
    long timeOutForJobFinish;
    Dag.DagNode<JobExecutionPlan> dagNode = this.dagManagementStateStore.getDag(dagAction.getDagId()).get().getNodes().get(0);

    try {
      timeOutForJobFinish = DagManagerUtils.getFlowSLA(dagNode);
    } catch (ConfigException e) {
      log.warn("Flow SLA for flowGroup: {}, flowName: {} is given in invalid format, using default SLA of {}",
          dagNode.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY),
          dagNode.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY),
          DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS);
      timeOutForJobFinish = DagManagerUtils.DEFAULT_FLOW_SLA_MILLIS;
    }

    long flowStartTime = DagManagerUtils.getFlowStartTime(dagNode);
    long reminderDuration = flowStartTime + timeOutForJobFinish - System.currentTimeMillis();

    dagActionReminderScheduler.get().scheduleReminder(dagAction, reminderDuration);
  }

  /**
   * Returns a {@link LeaseAttemptStatus} associated with the
   * `dagAction` by calling
   * {@link MultiActiveLeaseArbiter#tryAcquireLease(DagActionStore.DagAction, long, boolean, boolean)}.
   * @param dagAction
   * @return
   * @throws IOException
   * @throws SchedulerException
   */
  private LeaseAttemptStatus retrieveLeaseStatus(DagActionStore.DagAction dagAction)
      throws IOException, SchedulerException {
    // TODO: need to handle reminder events and flag them
    LeaseAttemptStatus leaseAttemptStatus = this.dagActionProcessingLeaseArbiter
        .tryAcquireLease(dagAction, System.currentTimeMillis(), dagAction.isReminder, false);
        /* Schedule a reminder for the event unless the lease has been completed to safeguard against the case where even
        we, when we might become the lease owner still fail to complete processing
        */
    if (!(leaseAttemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus)) {
      scheduleReminderForEvent(leaseAttemptStatus);
    }
    return leaseAttemptStatus;
  }

  private DagTask createDagTask(DagActionStore.DagAction dagAction, LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus) {
    DagActionStore.DagActionType dagActionType = dagAction.getDagActionType();

    switch (dagActionType) {
      case ENFORCE_FLOW_FINISH_DEADLINE:
        return new EnforceFlowFinishDeadlineDagTask(dagAction, leaseObtainedStatus, dagActionStore.get());
      case ENFORCE_JOB_START_DEADLINE:
        return new EnforceJobStartDeadlineDagTask(dagAction, leaseObtainedStatus, dagActionStore.get());
      case KILL:
        return new KillDagTask(dagAction, leaseObtainedStatus, dagActionStore.get());
      case LAUNCH:
        return new LaunchDagTask(dagAction, leaseObtainedStatus, dagActionStore.get());
      case REEVALUATE:
        return new ReevaluateDagTask(dagAction, leaseObtainedStatus, dagActionStore.get());
      case RESUME:
        return new ResumeDagTask(dagAction, leaseObtainedStatus, dagActionStore.get());
      default:
        throw new UnsupportedOperationException(dagActionType + " not yet implemented");
    }
  }

  /* Schedules a reminder for the flow action using {@link DagActionReminderScheduler} to reattempt the lease after the
  current leaseholder's grant would have expired.
  */
  protected void scheduleReminderForEvent(LeaseAttemptStatus leaseStatus)
      throws SchedulerException {
    dagActionReminderScheduler.get().scheduleReminder(leaseStatus.getConsensusDagAction(),
        leaseStatus.getMinimumLingerDurationMillis());
  }
}
