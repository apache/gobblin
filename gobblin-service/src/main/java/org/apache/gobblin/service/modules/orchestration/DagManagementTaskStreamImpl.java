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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.quartz.SchedulerException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
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
  protected MultiActiveLeaseArbiter dagActionProcessingLeaseArbiter;
  protected DagActionReminderScheduler dagActionReminderScheduler;
  private static final int MAX_HOUSEKEEPING_THREAD_DELAY = 180;
  private final BlockingQueue<DagActionStore.LeaseParams> leaseParamsQueue = new LinkedBlockingQueue<>();
  private final DagManagementStateStore dagManagementStateStore;
  private final DagProcessingEngineMetrics dagProcEngineMetrics;

  @Inject
  public DagManagementTaskStreamImpl(Config config, MultiActiveLeaseArbiter dagActionProcessingLeaseArbiter,
      DagActionReminderScheduler dagActionReminderScheduler, DagManagementStateStore dagManagementStateStore,
      DagProcessingEngineMetrics dagProcEngineMetrics) {
    this.config = config;
    this.dagActionProcessingLeaseArbiter = dagActionProcessingLeaseArbiter;
    this.dagActionReminderScheduler = dagActionReminderScheduler;
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(ConfigFactory.empty()), getClass());
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
    this.dagManagementStateStore = dagManagementStateStore;
    this.dagProcEngineMetrics = dagProcEngineMetrics;
  }

  @Override
  public synchronized void addDagAction(DagActionStore.LeaseParams leaseParams) {
    log.info("Adding {} to queue...", leaseParams);
    if (!this.leaseParamsQueue.offer(leaseParams)) {
      throw new RuntimeException(String.format("Could not add %s to the queue", leaseParams));
    }
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public DagTask next() {
      while (true) {
        DagActionStore.DagAction dagAction = null;
        try {
          DagActionStore.LeaseParams leaseParams = this.leaseParamsQueue.take();
          dagAction = leaseParams.getDagAction();
          /* Create triggers for original (non-reminder) dag actions of type ENFORCE_JOB_START_DEADLINE and ENFORCE_FLOW_FINISH_DEADLINE.
             Reminder triggers are used to inform hosts once the job start deadline and flow finish deadline are passed;
             then only is lease arbitration done to enforce the deadline violation and fail the job or flow if needed */
          if (!leaseParams.isReminder() && dagAction.dagActionType == DagActionStore.DagActionType.ENFORCE_JOB_START_DEADLINE) {
            createJobStartDeadlineTrigger(leaseParams);
          } else if (!leaseParams.isReminder() && dagAction.dagActionType == DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE) {
            createFlowFinishDeadlineTrigger(leaseParams);
          } else { // Handle original non-deadline dagActions as well as reminder events of all types
            LeaseAttemptStatus leaseAttemptStatus = retrieveLeaseStatus(leaseParams);
            if (leaseAttemptStatus instanceof LeaseAttemptStatus.LeaseObtainedStatus) {
              this.dagProcEngineMetrics.markDagActionsLeasedObtained(leaseParams);
              if (leaseParams.isReminder()) {
                this.dagProcEngineMetrics.markDagActionsRemindersProcessed(leaseParams);
              }
              return createDagTask(dagAction, (LeaseAttemptStatus.LeaseObtainedStatus) leaseAttemptStatus);
            }
          }
        } catch (Exception e) {
          //TODO: add metrics
          log.error("Exception getting DagAction from the queue or creating DagTask. dagAction - {}", dagAction == null ? "<null>" : dagAction, e);
        }
      }
  }

  private void createJobStartDeadlineTrigger(DagActionStore.LeaseParams leaseParams)
      throws SchedulerException, IOException {
    long timeOutForJobStart = DagUtils.getJobStartSla(this.dagManagementStateStore.getDag(
        leaseParams.getDagAction().getDagId()).get().getNodes().get(0), DagProcessingEngine.getDefaultJobStartSlaTimeMillis());
    // todo - this timestamp is just an approximation, the real job submission has happened in past, and that is when a
    // ENFORCE_JOB_START_DEADLINE dag action was created; we are just processing that dag action here
    long jobSubmissionTime = System.currentTimeMillis();
    long reminderDuration = jobSubmissionTime + timeOutForJobStart - System.currentTimeMillis();

    dagActionReminderScheduler.scheduleReminder(leaseParams, reminderDuration, true);
  }

  private void createFlowFinishDeadlineTrigger(DagActionStore.LeaseParams leaseParams)
      throws SchedulerException, IOException {
    long timeOutForJobFinish;
    Dag.DagNode<JobExecutionPlan> dagNode = this.dagManagementStateStore.getDag(leaseParams.getDagAction().getDagId()).get().getNodes().get(0);

    try {
      timeOutForJobFinish = DagUtils.getFlowSLA(dagNode);
    } catch (ConfigException e) {
      log.warn("Flow SLA for flowGroup: {}, flowName: {} is given in invalid format, using default SLA of {}",
          dagNode.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_GROUP_KEY),
          dagNode.getValue().getJobSpec().getConfig().getString(ConfigurationKeys.FLOW_NAME_KEY),
          ServiceConfigKeys.DEFAULT_FLOW_SLA_MILLIS);
      timeOutForJobFinish = ServiceConfigKeys.DEFAULT_FLOW_SLA_MILLIS;
    }

    long flowStartTime = DagUtils.getFlowStartTime(dagNode);
    long reminderDuration = flowStartTime + timeOutForJobFinish - System.currentTimeMillis();

    dagActionReminderScheduler.scheduleReminder(leaseParams, reminderDuration, true);
  }

  /**
   * Returns a {@link LeaseAttemptStatus} associated with the
   * `dagAction` by calling
   * {@link MultiActiveLeaseArbiter#tryAcquireLease(DagActionStore.LeaseParams, boolean)}.
   * @param leaseParams
   * @return
   * @throws IOException
   * @throws SchedulerException
   */
  private LeaseAttemptStatus retrieveLeaseStatus(DagActionStore.LeaseParams leaseParams)
      throws IOException, SchedulerException {
    // Uses reminder flag to determine whether to use current time as event time or previously saved event time
    LeaseAttemptStatus leaseAttemptStatus = this.dagActionProcessingLeaseArbiter
        .tryAcquireLease(leaseParams, false);
        /* Schedule a reminder for the event unless the lease has been completed to safeguard against the case where
        even we, when we might become the lease owner still fail to complete processing
        */
    if (!(leaseAttemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus)) {
      scheduleReminderForEvent(leaseAttemptStatus);
      this.dagProcEngineMetrics.markDagActionsLeaseReminderScheduled(leaseParams);
    } else {
      this.dagProcEngineMetrics.markDagActionsNoLongerLeasing(leaseParams);
    }
    return leaseAttemptStatus;
  }

  private DagTask createDagTask(DagActionStore.DagAction dagAction, LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus) {
    DagActionStore.DagActionType dagActionType = dagAction.getDagActionType();

    switch (dagActionType) {
      case ENFORCE_FLOW_FINISH_DEADLINE:
        return new EnforceFlowFinishDeadlineDagTask(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
      case ENFORCE_JOB_START_DEADLINE:
        return new EnforceJobStartDeadlineDagTask(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
      case KILL:
        return new KillDagTask(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
      case LAUNCH:
        return new LaunchDagTask(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
      case REEVALUATE:
        return new ReevaluateDagTask(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
      case RESUME:
        return new ResumeDagTask(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
      default:
        throw new UnsupportedOperationException(dagActionType + " not yet implemented");
    }
  }

  /* Schedules a reminder for the flow action using {@link DagActionReminderScheduler} to reattempt the lease after the
  current leaseholder's grant would have expired. It saves the previous eventTimeMillis in the dagAction to use upon
  reattempting the lease.
  */
  protected void scheduleReminderForEvent(LeaseAttemptStatus leaseStatus)
      throws SchedulerException {
    dagActionReminderScheduler.scheduleReminder(leaseStatus.getConsensusLeaseParams(),
        leaseStatus.getMinimumLingerDurationMillis(), false);
  }
}
