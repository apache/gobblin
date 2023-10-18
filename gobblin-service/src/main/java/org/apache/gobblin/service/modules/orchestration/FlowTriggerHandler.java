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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.runtime.api.MysqlMultiActiveLeaseArbiter;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.util.ConfigUtils;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.JobDetailImpl;


/**
 * Handler used to coordinate multiple hosts with enabled schedulers to respond to flow action events. It uses the
 * {@link MysqlMultiActiveLeaseArbiter} to determine a single lease owner at a given time
 * for a flow action event. After acquiring the lease, it persists the flow action event to the {@link DagActionStore}
 * to be eventually acted upon by the host with the active DagManager. Once it has completed this action, it will mark
 * the lease as completed by calling the
 * {@link MysqlMultiActiveLeaseArbiter.recordLeaseSuccess()} method. Hosts that do not gain the lease for the event,
 * instead schedule a reminder using the {@link SchedulerService} to check back in on the previous lease owner's
 * completion status after the lease should expire to ensure the event is handled in failure cases.
 */
@Slf4j
public class FlowTriggerHandler {
  private final int schedulerMaxBackoffMillis;
  private static Random random = new Random();
  protected Optional<MultiActiveLeaseArbiter> multiActiveLeaseArbiter;
  protected SchedulerService schedulerService;
  protected Optional<DagActionStore> dagActionStore;
  private MetricContext metricContext;
  private ContextAwareMeter numFlowsSubmitted;

  private ContextAwareCounter leaseObtainedCount;

  private ContextAwareCounter leasedToAnotherStatusCount;

  private ContextAwareCounter noLongerLeasingStatusCount;
  private ContextAwareCounter jobDoesNotExistInSchedulerCount;
  private ContextAwareCounter failedToSetEventReminderCount;

  @Inject
  public FlowTriggerHandler(Config config, Optional<MultiActiveLeaseArbiter> leaseDeterminationStore,
      SchedulerService schedulerService, Optional<DagActionStore> dagActionStore) {
    this.schedulerMaxBackoffMillis = ConfigUtils.getInt(config, ConfigurationKeys.SCHEDULER_MAX_BACKOFF_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_MAX_BACKOFF_MILLIS);
    this.multiActiveLeaseArbiter = leaseDeterminationStore;
    this.schedulerService = schedulerService;
    this.dagActionStore = dagActionStore;
    this.metricContext = Instrumented.getMetricContext(new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)),
        this.getClass());
    this.numFlowsSubmitted = metricContext.contextAwareMeter(ServiceMetricNames.GOBBLIN_FLOW_TRIGGER_HANDLER_NUM_FLOWS_SUBMITTED);
    this.leaseObtainedCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASE_OBTAINED_COUNT);
    this.leasedToAnotherStatusCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_LEASED_TO_ANOTHER_COUNT);
    this.noLongerLeasingStatusCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_NO_LONGER_LEASING_COUNT);
    this.jobDoesNotExistInSchedulerCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_JOB_DOES_NOT_EXIST_COUNT);
    this.failedToSetEventReminderCount = this.metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_FAILED_TO_SET_REMINDER_COUNT);
  }

  /**
   * This method is used in the multi-active scheduler case for one or more hosts to respond to a flow action event
   * by attempting a lease for the flow event and processing the result depending on the status of the attempt.
   * @param jobProps
   * @param flowAction
   * @param eventTimeMillis
   * @param isReminderEvent
   * @throws IOException
   */
  public void handleTriggerEvent(Properties jobProps, DagActionStore.DagAction flowAction, long eventTimeMillis,
      boolean isReminderEvent) throws IOException {
    if (multiActiveLeaseArbiter.isPresent()) {
      MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = multiActiveLeaseArbiter.get().tryAcquireLease(
          flowAction, eventTimeMillis, isReminderEvent);
      // The flow action contained in the`LeaseAttemptStatus` from the lease arbiter contains an updated flow execution
      // id. From this point onwards, always use the newer version of the flow action to easily track the action through
      // orchestration and execution.
      if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
        MultiActiveLeaseArbiter.LeaseObtainedStatus leaseObtainedStatus = (MultiActiveLeaseArbiter.LeaseObtainedStatus)
            leaseAttemptStatus;
        this.leaseObtainedCount.inc();
        if (persistFlowAction(leaseObtainedStatus)) {
          log.info("Successfully persisted lease: [{}, eventTimestamp: {}] ", leaseObtainedStatus.getFlowAction(),
              leaseObtainedStatus.getEventTimeMillis());
          return;
        }
        // If persisting the flow action failed, then we set another trigger for this event to occur immediately to
        // re-attempt handling the event
        scheduleReminderForEvent(jobProps,
            new MultiActiveLeaseArbiter.LeasedToAnotherStatus(leaseObtainedStatus.getFlowAction(),
                0L), eventTimeMillis);
        return;
      } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeasedToAnotherStatus) {
        this.leasedToAnotherStatusCount.inc();
        scheduleReminderForEvent(jobProps,
            (MultiActiveLeaseArbiter.LeasedToAnotherStatus) leaseAttemptStatus, eventTimeMillis);
        return;
      } else if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.NoLongerLeasingStatus) {
        this.noLongerLeasingStatusCount.inc();
        log.debug("Received type of leaseAttemptStatus: [{}, eventTimestamp: {}] ", leaseAttemptStatus.getClass().getName(),
            eventTimeMillis);
        return;
      }
      throw new RuntimeException(String.format("Received type of leaseAttemptStatus: %s not handled by this method",
          leaseAttemptStatus.getClass().getName()));
    } else {
      throw new RuntimeException(String.format("Multi-active scheduler is not enabled so trigger event should not be "
          + "handled with this method."));
    }
  }

  // Called after obtaining a lease to persist the flow action to {@link DagActionStore} and mark the lease as done
  private boolean persistFlowAction(MultiActiveLeaseArbiter.LeaseObtainedStatus leaseStatus) {
    if (this.dagActionStore.isPresent() && this.multiActiveLeaseArbiter.isPresent()) {
      try {
        DagActionStore.DagAction flowAction = leaseStatus.getFlowAction();
        this.dagActionStore.get().addDagAction(flowAction.getFlowGroup(), flowAction.getFlowName(), flowAction.getFlowExecutionId(), flowAction.getFlowActionType());
        // If the flow action has been persisted to the {@link DagActionStore} we can close the lease
        this.numFlowsSubmitted.mark();
        return this.multiActiveLeaseArbiter.get().recordLeaseSuccess(leaseStatus);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("DagActionStore is " + (this.dagActionStore.isPresent() ? "" : "NOT") + " present. "
          + "Multi-Active scheduler is " + (this.multiActiveLeaseArbiter.isPresent() ? "" : "NOT") + " present. Both "
          + "should be enabled if this method is called.");
    }
  }

  /**
   * This method is used by {@link FlowTriggerHandler.handleTriggerEvent} to schedule a self-reminder to check on
   * the other participant's progress to finish acting on a flow action after the time the lease should expire.
   * @param jobProps
   * @param status used to extract event to be reminded for and the minimum time after which reminder should occur
   * @param triggerEventTimeMillis the event timestamp we were originally handling
   */
  private void scheduleReminderForEvent(Properties jobProps, MultiActiveLeaseArbiter.LeasedToAnotherStatus status,
      long triggerEventTimeMillis) {
    DagActionStore.DagAction flowAction = status.getFlowAction();
    JobKey origJobKey = new JobKey(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "<<no job name>>"),
        jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, "<<no job group>>"));
    try {
      if (!this.schedulerService.getScheduler().checkExists(origJobKey)) {
        log.warn("Skipping setting a reminder for a job that does not exist in the scheduler. Key: {}", origJobKey);
        this.jobDoesNotExistInSchedulerCount.inc();
        return;
      }
      Trigger reminderTrigger = createAndScheduleReminder(origJobKey, status, triggerEventTimeMillis);
      log.info("Flow Trigger Handler - [{}, eventTimestamp: {}] - SCHEDULED REMINDER for event {} in {} millis",
          flowAction, triggerEventTimeMillis, status.getEventTimeMillis(), reminderTrigger.getNextFireTime());
    } catch (SchedulerException e) {
      log.warn("Failed to add job reminder due to SchedulerException for job {} trigger event {}. Exception: {}",
          origJobKey, status.getEventTimeMillis(), e);
      this.failedToSetEventReminderCount.inc();
    }
  }

  /**
   * Create a new trigger with a `reminder` suffix that is set to fire at the minimum reminder wait time calculated from
   * the LeasedToAnotherStatus provided by the caller. The new trigger and job will store the original
   * triggerEventTimeMillis to revisit upon firing.
   * @param origJobKey
   * @param status
   * @param triggerEventTimeMillis
   * @return Trigger for reminder
   * @throws SchedulerException
   */
  protected Trigger createAndScheduleReminder(JobKey origJobKey, MultiActiveLeaseArbiter.LeasedToAnotherStatus status,
      long triggerEventTimeMillis) throws SchedulerException {
    // Generate a suffix to differentiate the reminder Job and Trigger from the original JobKey and Trigger, so we can
    // allow us to keep track of additional properties needed for reminder events (all triggers associated with one job
    // refer to the same set of jobProperties)
    String reminderSuffix = createSuffixForJobTrigger(status);
    JobKey reminderJobKey = new JobKey(origJobKey.getName() + reminderSuffix, origJobKey.getGroup());
    JobDetailImpl jobDetail = createJobDetailForReminderEvent(origJobKey, reminderJobKey, status);
    Trigger reminderTrigger = JobScheduler.createTriggerForJob(reminderJobKey, getJobPropertiesFromJobDetail(jobDetail),
        Optional.of(reminderSuffix));
    log.debug("Flow Trigger Handler - [{}, eventTimestamp: {}] -  attempting to schedule reminder for event {} with "
            + "reminderJobKey {} and reminderTriggerKey {}", status.getFlowAction(), triggerEventTimeMillis,
        status.getEventTimeMillis(), reminderJobKey, reminderTrigger.getKey());
    this.schedulerService.getScheduler().scheduleJob(jobDetail, reminderTrigger);
    return reminderTrigger;
  }

  /**
   * Create suffix to add to end of flow name to differentiate reminder triggers from the original job schedule trigger
   * and ensure they are added to the scheduler.
   * @param leasedToAnotherStatus
   * @return
   */
  @VisibleForTesting
  public static String createSuffixForJobTrigger(MultiActiveLeaseArbiter.LeasedToAnotherStatus leasedToAnotherStatus) {
    return "reminder_for_" + leasedToAnotherStatus.getEventTimeMillis();
  }

  /**
   * Helper function used to extract JobDetail for job identified by the originalKey and update it be associated with
   * the event to revisit. It will update the jobKey to the reminderKey provides and the Properties map to
   * contain the cron scheduler for the reminder event and information about the event to revisit
   * @param originalKey
   * @param reminderKey
   * @param status
   * @return
   * @throws SchedulerException
   */
  protected JobDetailImpl createJobDetailForReminderEvent(JobKey originalKey, JobKey reminderKey,
      MultiActiveLeaseArbiter.LeasedToAnotherStatus status)
      throws SchedulerException {
    JobDetailImpl jobDetail = (JobDetailImpl) this.schedulerService.getScheduler().getJobDetail(originalKey);
    jobDetail.setKey(reminderKey);
    JobDataMap jobDataMap = jobDetail.getJobDataMap();
    jobDataMap = updatePropsInJobDataMap(jobDataMap, status, schedulerMaxBackoffMillis);
    jobDetail.setJobDataMap(jobDataMap);
    return jobDetail;
  }

  public static Properties getJobPropertiesFromJobDetail(JobDetail jobDetail) {
    return (Properties) jobDetail.getJobDataMap().get(GobblinServiceJobScheduler.PROPERTIES_KEY);
  }

  /**
   * Updates the cronExpression, reminderTimestamp, originalEventTime values in the properties map of a JobDataMap
   * provided returns the updated JobDataMap to the user
   * @param jobDataMap
   * @param leasedToAnotherStatus
   * @param schedulerMaxBackoffMillis
   * @return
   */
  @VisibleForTesting
  public static JobDataMap updatePropsInJobDataMap(JobDataMap jobDataMap,
      MultiActiveLeaseArbiter.LeasedToAnotherStatus leasedToAnotherStatus, int schedulerMaxBackoffMillis) {
    Properties prevJobProps = (Properties) jobDataMap.get(GobblinServiceJobScheduler.PROPERTIES_KEY);
    // Add a small randomization to the minimum reminder wait time to avoid 'thundering herd' issue
    long delayPeriodMillis = leasedToAnotherStatus.getMinimumLingerDurationMillis()
        + random.nextInt(schedulerMaxBackoffMillis);
    String cronExpression = createCronFromDelayPeriod(delayPeriodMillis);
    prevJobProps.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, cronExpression);
    // Saves the following properties in jobProps to retrieve when the trigger fires
    prevJobProps.setProperty(ConfigurationKeys.SCHEDULER_EXPECTED_REMINDER_TIME_MILLIS_KEY,
        String.valueOf(getUTCTimeFromDelayPeriod(delayPeriodMillis)));
    // Use the db laundered timestamp for the reminder to ensure consensus between hosts. Participant trigger timestamps
    // can differ between participants and be interpreted as a reminder for a distinct flow trigger which will cause
    // excess flows to be triggered by the reminder functionality.
    prevJobProps.setProperty(ConfigurationKeys.SCHEDULER_PRESERVED_CONSENSUS_EVENT_TIME_MILLIS_KEY,
        String.valueOf(leasedToAnotherStatus.getEventTimeMillis()));
    // Use this boolean to indicate whether this is a reminder event
    prevJobProps.setProperty(ConfigurationKeys.FLOW_IS_REMINDER_EVENT_KEY, String.valueOf(true));
    // Update job data map and reset it in jobDetail
    jobDataMap.put(GobblinServiceJobScheduler.PROPERTIES_KEY, prevJobProps);
    return jobDataMap;
  }

  /**
   * Create a cron expression for the time that is delay milliseconds in the future
   * @param delayPeriodMillis
   * @return String representing cron schedule
   */
  protected static String createCronFromDelayPeriod(long delayPeriodMillis) {
    LocalDateTime timeToScheduleReminder = getLocalDateTimeFromDelayPeriod(delayPeriodMillis);
    // TODO: investigate potentially better way of generating cron expression that does not make it US dependent
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ss mm HH dd MM ? yyyy", Locale.US);
    return timeToScheduleReminder.format(formatter);
  }

  /**
   * Returns a LocalDateTime in UTC timezone that is delay milliseconds in the future
   */
  protected static LocalDateTime getLocalDateTimeFromDelayPeriod(long delayPeriodMillis) {
    LocalDateTime now = LocalDateTime.now(ZoneId.of("UTC"));
    return now.plus(delayPeriodMillis, ChronoUnit.MILLIS);
  }

  /**
   * Takes a given delay period in milliseconds and returns the number of millseconds since epoch from current time
   */
  protected static long getUTCTimeFromDelayPeriod(long delayPeriodMillis) {
    LocalDateTime localDateTime = getLocalDateTimeFromDelayPeriod(delayPeriodMillis);
    Date date = Date.from(localDateTime.atZone(ZoneId.of("UTC")).toInstant());
    return GobblinServiceJobScheduler.utcDateAsUTCEpochMillis(date);
  }
}
