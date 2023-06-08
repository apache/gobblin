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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Handler used to coordinate multiple hosts with enabled schedulers to respond to flow action events. It uses the
 * {@link MysqlMultiActiveLeaseArbiter} to determine a single lease owner at a given time
 * for a flow action event. After acquiring the lease, it persists the flow action event to the {@link DagActionStore}
 * to be eventually acted upon by the host with the active DagManager. Once it has completed this action, it will mark
 * the lease as completed by calling the
 * MysqlMultiActiveLeaseArbiter.recordLeaseSuccess method. Hosts that do not gain
 * the lease for the event, instead schedule a reminder using the {@link SchedulerService} to check back in on the
 * previous lease owner's completion status after the lease should expire to ensure the event is handled in failure
 * cases.
 */
@Slf4j
public class FlowTriggerHandler {
  private final int schedulerMaxBackoffMillis;
  private static Random random = new Random();
  protected MultiActiveLeaseArbiter multiActiveLeaseArbiter;
  protected JobScheduler jobScheduler;
  protected SchedulerService schedulerService;
  protected DagActionStore dagActionStore;
  private MetricContext metricContext;
  private ContextAwareMeter numLeasesCompleted;

  @Inject
  // TODO: should multiActiveLeaseArbiter and DagActionStore be optional?
  public FlowTriggerHandler(Config config, MultiActiveLeaseArbiter leaseDeterminationStore,
      JobScheduler jobScheduler, SchedulerService schedulerService, DagActionStore dagActionStore) {
    this.schedulerMaxBackoffMillis = ConfigUtils.getInt(config, ConfigurationKeys.SCHEDULER_MAX_BACKOFF_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_MAX_BACKOFF_MILLIS);
    this.multiActiveLeaseArbiter = leaseDeterminationStore;
    this.jobScheduler = jobScheduler;
    this.schedulerService = schedulerService;
    this.dagActionStore = dagActionStore;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass());
    this.numLeasesCompleted = metricContext.contextAwareMeter(RuntimeMetrics.GOBBLIN_SCHEDULER_LEASE_ALGO_HANDLER_NUM_FLOWS_SUBMITTED);
  }

  /**
   * This method is used in the multi-active scheduler case for one or more hosts to respond to a flow action event
   * by attempting a lease for the flow event and processing the result depending on the status of the attempt.
   * @param jobProps
   * @param flowAction
   * @param eventTimeMillis
   * @throws IOException
   */
  public void handleTriggerEvent(Properties jobProps, DagActionStore.DagAction flowAction, long eventTimeMillis)
      throws IOException {
    MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus =
        multiActiveLeaseArbiter.tryAcquireLease(flowAction, eventTimeMillis);
    // TODO: add a log event or metric for each of these cases
    if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
      persistFlowAction((MultiActiveLeaseArbiter.LeaseObtainedStatus) leaseAttemptStatus, flowAction);
      return;
    } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus) {
      scheduleReminderForEvent(jobProps, (MultiActiveLeaseArbiter.LeasedToAnotherStatus) leaseAttemptStatus, flowAction,
          eventTimeMillis);
    } else if (leaseAttemptStatus instanceof  MultiActiveLeaseArbiter.NoLongerLeasingStatus) {
      return;
    }
    log.warn("Received type of leaseAttemptStatus: {} not handled by this method", leaseAttemptStatus.getClass().getName());
  }

  // Called after obtaining a lease to persist the flow action to {@link DagActionStore} and mark the lease as done
  private boolean persistFlowAction(MultiActiveLeaseArbiter.LeaseObtainedStatus status, DagActionStore.DagAction flowAction) {
    try {
      this.dagActionStore.addDagAction(flowAction.getFlowGroup(), flowAction.getFlowName(),
          flowAction.getFlowExecutionId(), flowAction.getFlowActionType());
      // If the flow action has been persisted to the {@link DagActionStore} we can close the lease
      this.numLeasesCompleted.mark();
      return this.multiActiveLeaseArbiter.recordLeaseSuccess(flowAction, status);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method is used by FlowTriggerHandler.handleNewSchedulerEvent to schedule a reminder for itself to check on
   * the other participant's progress to finish acting on a flow action after the time the lease should expire.
   * @param jobProps
   * @param status used to extract event to be reminded for and the minimum time after which reminder should occur
   * @param originalEventTimeMillis the event timestamp we were originally handling
   * @param flowAction
   */
  private void scheduleReminderForEvent(Properties jobProps, MultiActiveLeaseArbiter.LeasedToAnotherStatus status,
      DagActionStore.DagAction flowAction, long originalEventTimeMillis) {
    // Add a small randomization to the minimum reminder wait time to avoid 'thundering herd' issue
    String cronExpression = createCronFromDelayPeriod(status.getMinimumLingerDurationMillis()
        + random.nextInt(schedulerMaxBackoffMillis));
    jobProps.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, cronExpression);
    // Ensure we save the event timestamp that we're setting reminder for to have for debugging purposes
    // in addition to the event we want to initiate
    jobProps.setProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_REVISIT_TIMESTAMP_MILLIS_KEY,
        String.valueOf(status.getEventTimeMillis()));
    jobProps.setProperty(ConfigurationKeys.SCHEDULER_EVENT_TO_TRIGGER_TIMESTAMP_MILLIS_KEY,
        String.valueOf(originalEventTimeMillis));
    JobKey key = new JobKey(flowAction.getFlowName(), flowAction.getFlowGroup());
    // Create a new trigger for the flow in job scheduler that is set to fire at the minimum reminder wait time calculated
    Trigger trigger = this.jobScheduler.createTriggerForJob(key, jobProps);
    try {
      log.info("Scheduler Lease Algo Handler - [%s, eventTimestamp: %s] -  attempting to schedule reminder for event %s in %s millis",
          flowAction, originalEventTimeMillis, status.getEventTimeMillis(), trigger.getNextFireTime());
      this.schedulerService.getScheduler().scheduleJob(trigger);
    } catch (SchedulerException e) {
      log.warn("Failed to add job reminder due to SchedulerException for job %s trigger event %s ", key, status.getEventTimeMillis(), e);
    }
    log.info(String.format("Scheduler Lease Algo Handler - [%s, eventTimestamp: %s] - SCHEDULED REMINDER for event %s in %s millis",
        flowAction, originalEventTimeMillis, status.getEventTimeMillis(), trigger.getNextFireTime()));
  }

  /**
   * These methods should only be called from the Orchestrator or JobScheduler classes as it directly adds jobs to the
   * Quartz scheduler
   * @param delayPeriodMillis
   * @return
   */
  protected static String createCronFromDelayPeriod(long delayPeriodMillis) {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    LocalDateTime delaySecondsLater = now.plus(delayPeriodMillis, ChronoUnit.MILLIS);
    // TODO: investigate potentially better way of generating cron expression that does not make it US dependent
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ss mm HH dd MM ? yyyy", Locale.US);
    return delaySecondsLater.format(formatter);
  }
}
