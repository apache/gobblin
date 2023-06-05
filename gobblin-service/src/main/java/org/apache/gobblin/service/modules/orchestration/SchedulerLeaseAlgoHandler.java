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
import java.sql.SQLException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import javax.inject.Inject;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.LeaseAttemptStatus;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.runtime.api.LeaseObtainedStatus;
import org.apache.gobblin.runtime.api.LeasedToAnotherStatus;


/**
 * Handler used to coordinate multiple hosts with enabled schedulers to respond to flow action events. It uses the
 * {@link org.apache.gobblin.runtime.api.MySQLMultiActiveLeaseArbiter} to determine a single lease owner at a given time
 * for a flow action event. After acquiring the lease, it persists the flow action event to the {@link DagActionStore}
 * to be eventually acted upon by the host with the active DagManager. Once it has completed this action, it will mark
 * the lease as completed by calling the
 * {@link org.apache.gobblin.runtime.api.MySQLMultiActiveLeaseArbiter.completeLeaseUse} method. Hosts that do not gain
 * the lease for the event, instead schedule a reminder using the {@link SchedulerService} to check back in on the
 * previous lease owner's completion status after the lease should expire to ensure the event is handled in failure
 * cases.
 */
public class SchedulerLeaseAlgoHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerLeaseAlgoHandler.class);
  private final int staggerUpperBoundSec;
  private static Random random = new Random();
  protected MultiActiveLeaseArbiter multiActiveLeaseArbiter;
  protected JobScheduler jobScheduler;
  protected SchedulerService schedulerService;
  protected DagActionStore dagActionStore;
  private MetricContext metricContext;
  private ContextAwareMeter numLeasesCompleted;
  @Inject
  public SchedulerLeaseAlgoHandler(Config config, MultiActiveLeaseArbiter leaseDeterminationStore,
      JobScheduler jobScheduler, SchedulerService schedulerService, DagActionStore dagActionStore) {
    this.staggerUpperBoundSec = ConfigUtils.getInt(config,
        ConfigurationKeys.SCHEDULER_STAGGERING_UPPER_BOUND_SEC_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_STAGGERING_UPPER_BOUND_SEC);
    this.multiActiveLeaseArbiter = leaseDeterminationStore;
    this.jobScheduler = jobScheduler;
    this.schedulerService = schedulerService;
    this.dagActionStore = dagActionStore;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass());
    this.numLeasesCompleted = metricContext.contextAwareMeter(RuntimeMetrics.GOBBLIN_SCHEDULER_LEASE_ALGO_HANDLER_NUM_LEASES_COMPLETED);
  }

  /**
   * This method is used in the multi-active scheduler case for one or more hosts to respond to a flow action event
   * by attempting a lease for the flow event and processing the result depending on the status of the attempt.
   * @param jobProps
   * @param flowAction
   * @param eventTimeMillis
   * @throws IOException
   */
  public void handleNewSchedulerEvent(Properties jobProps, DagActionStore.DagAction flowAction, long eventTimeMillis)
      throws IOException {
    LeaseAttemptStatus leaseAttemptStatus =
        multiActiveLeaseArbiter.tryAcquireLease(flowAction, eventTimeMillis);
    // TODO: add a log event or metric for each of these cases
    switch (leaseAttemptStatus.getClass().getSimpleName()) {
      case "LeaseObtainedStatus":
        finalizeLease((LeaseObtainedStatus) leaseAttemptStatus, flowAction);
        break;
      case "LeasedToAnotherStatus":
        scheduleReminderForEvent(jobProps, (LeasedToAnotherStatus) leaseAttemptStatus, flowAction, eventTimeMillis);
        break;
      case "NoLongerLeasingStatus":
        break;
      default:
    }
  }

  // Called after obtaining a lease to persist the flow action to {@link DagActionStore} and mark the lease as done
  private boolean finalizeLease(LeaseObtainedStatus status, DagActionStore.DagAction flowAction) {
    try {
      this.dagActionStore.addDagAction(flowAction.getFlowGroup(), flowAction.getFlowName(),
          flowAction.getFlowExecutionId(), flowAction.getFlowActionType());
      if (this.dagActionStore.exists(flowAction.getFlowGroup(), flowAction.getFlowName(),
          flowAction.getFlowExecutionId(), flowAction.getFlowActionType())) {
        // If the flow action has been persisted to the {@link DagActionStore} we can close the lease
        this.numLeasesCompleted.mark();
        return this.multiActiveLeaseArbiter.completeLeaseUse(flowAction, status.getEventTimestamp(),
            status.getMyLeaseAcquisitionTimestamp());
      }
    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
    // TODO: should this return an error or print a warning log if failed to commit to dag action store?
    return false;
  }

  /**
   * This method is used by {@link SchedulerLeaseAlgoHandler.handleNewSchedulerEvent} to schedule a reminder for itself
   * to check on the other participant's progress to finish acting on a flow action after the time the lease should
   * expire.
   * @param jobProps
   * @param status used to extract event to be reminded for and the minimum time after which reminder should occur
   * @param originalEventTimeMillis the event timestamp we were originally handling
   * @param flowAction
   */
  private void scheduleReminderForEvent(Properties jobProps, LeasedToAnotherStatus status,
      DagActionStore.DagAction flowAction, long originalEventTimeMillis) {
    // Add a small randomization to the minimum reminder wait time to avoid 'thundering herd' issue
    String cronExpression = createCronFromDelayPeriod(status.getMinimumReminderWaitMillis() + random.nextInt(staggerUpperBoundSec));
    jobProps.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, cronExpression);
    // Ensure we save the event timestamp that we're setting reminder for, in addition to our own event timestamp which may be different
    jobProps.setProperty(ConfigurationKeys.SCHEDULER_REMINDER_EVENT_TIMESTAMP_MILLIS_KEY, String.valueOf(status.getReminderEventTimeMillis()));
    jobProps.setProperty(ConfigurationKeys.SCHEDULER_NEW_EVENT_TIMESTAMP_MILLIS_KEY, String.valueOf(status.getReminderEventTimeMillis()));
    JobKey key = new JobKey(flowAction.getFlowName(), flowAction.getFlowGroup());
    Trigger trigger = this.jobScheduler.getTrigger(key, jobProps);
    try {
      LOG.info("Scheduler Lease Algo Handler - [%s, eventTimestamp: %s] -  attempting to schedule reminder for event %s in %s millis",
          flowAction, originalEventTimeMillis, status.getReminderEventTimeMillis(), trigger.getNextFireTime());
      this.schedulerService.getScheduler().scheduleJob(trigger);
    } catch (SchedulerException e) {
      LOG.warn("Failed to add job reminder due to SchedulerException for job %s trigger event %s ", key, status.getReminderEventTimeMillis(), e);
    }
    LOG.info(String.format("Scheduler Lease Algo Handler - [%s, eventTimestamp: %s] - SCHEDULED REMINDER for event %s in %s millis",
        flowAction, originalEventTimeMillis, status.getReminderEventTimeMillis(), trigger.getNextFireTime()));
  }

  /**
   * These methods should only be called from the Orchestrator or JobScheduler classes as it directly adds jobs to the
   * Quartz scheduler
   * @param delayPeriodSeconds
   * @return
   */
  protected static String createCronFromDelayPeriod(long delayPeriodSeconds) {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    LocalDateTime delaySecondsLater = now.plus(delayPeriodSeconds, ChronoUnit.SECONDS);
    // TODO: investigate potentially better way of generating cron expression that does not make it US dependent
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ss mm HH dd MM ? yyyy", Locale.US);
    return delaySecondsLater.format(formatter);
  }
}
