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
import java.util.Date;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.JobDetailImpl;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareCounter;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Handler used to coordinate multiple hosts with enabled schedulers to respond to launch dag action events and ensure
 * no adhoc or scheduled flow launches are missed. It uses the {@link MultiActiveLeaseArbiter} to determine a single
 * lease owner at a given time for the launch dag action event. After acquiring the lease, it persists the dag action
 * event to the {@link DagActionStore} to be eventually acted upon by execution module of host(s) to execute the launch.
 * Once it has completed persisting the action to the store, it will mark the lease as completed by calling the
 * {@link MultiActiveLeaseArbiter#recordLeaseSuccess(LeaseAttemptStatus.LeaseObtainedStatus)} method. Hosts
 * that do not gain the lease for the event, instead schedule a reminder using the {@link SchedulerService} to check
 * back in on the previous lease owner's completion status after the lease should expire to ensure the event is handled
 * in failure cases.
 */
@Slf4j
public class FlowLaunchHandler {
  private final MultiActiveLeaseArbiter multiActiveLeaseArbiter;
  private final DagManagementStateStore dagManagementStateStore;
  private final int schedulerMaxBackoffMillis;
  private static final Random random = new Random();
  protected SchedulerService schedulerService;
  private final ContextAwareMeter numFlowsSubmitted;
  private final ContextAwareCounter jobDoesNotExistInSchedulerCount;
  private final ContextAwareCounter failedToSetEventReminderCount;

  @Inject
  public FlowLaunchHandler(Config config, MultiActiveLeaseArbiter leaseArbiter,
      SchedulerService schedulerService, DagManagementStateStore dagManagementStateStore) {
    this.multiActiveLeaseArbiter = leaseArbiter;
    this.dagManagementStateStore = dagManagementStateStore;

    this.schedulerMaxBackoffMillis = ConfigUtils.getInt(config, ConfigurationKeys.SCHEDULER_MAX_BACKOFF_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_MAX_BACKOFF_MILLIS);
    this.schedulerService = schedulerService;

    // Initialize FlowLaunchHandler related metrics
    MetricContext metricContext = Instrumented.getMetricContext(
        new org.apache.gobblin.configuration.State(ConfigUtils.configToProperties(config)), this.getClass());
    this.numFlowsSubmitted = metricContext.contextAwareMeter(ServiceMetricNames.GOBBLIN_FLOW_TRIGGER_HANDLER_NUM_FLOWS_SUBMITTED);
    this.jobDoesNotExistInSchedulerCount = metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_JOB_DOES_NOT_EXIST_COUNT);
    this.failedToSetEventReminderCount = metricContext.contextAwareCounter(ServiceMetricNames.FLOW_TRIGGER_HANDLER_FAILED_TO_SET_REMINDER_COUNT);
  }

  /**
   * This method is used in the multi-active scheduler case for one or more hosts to respond to a launch dag action
   * event triggered by the scheduler by attempting a lease for the launch event and processing the result depending on
   * the status of the attempt.
   */
  public void handleFlowLaunchTriggerEvent(Properties jobProps,
      DagActionStore.LeaseParams leaseParams, boolean adoptConsensusFlowExecutionId)
      throws IOException {
    handleFlowTriggerEvent(jobProps, leaseParams, adoptConsensusFlowExecutionId);
  }

  /**
   * This method is used in the multi-active scheduler case for one or more hosts to respond to a kill dag action
   * event triggered by the Orchestrator by attempting a lease for the kill event and processing the result depending on
   * the status of the attempt.
   */
  public void handleFlowKillTriggerEvent(Properties jobProps, DagActionStore.LeaseParams leaseParams) throws IOException {
    handleFlowTriggerEvent(jobProps, leaseParams, false);
  }

  private void handleFlowTriggerEvent(Properties jobProps, DagActionStore.LeaseParams leaseParams, boolean adoptConsensusFlowExecutionId)
      throws IOException {
    long previousEventTimeMillis = leaseParams.getEventTimeMillis();
    LeaseAttemptStatus leaseAttempt = this.multiActiveLeaseArbiter.tryAcquireLease(leaseParams, adoptConsensusFlowExecutionId);
    if (leaseAttempt instanceof LeaseAttemptStatus.LeaseObtainedStatus
        && persistLaunchDagAction((LeaseAttemptStatus.LeaseObtainedStatus) leaseAttempt)) {
      log.info("Successfully persisted lease: [{}, eventTimestamp: {}] ", leaseAttempt.getConsensusDagAction(),
          previousEventTimeMillis);
    } else { // when NOT successfully `persistDagAction`, set a reminder to re-attempt handling (unless leasing finished)
      calcLeasedToAnotherStatusForReminder(leaseAttempt).ifPresent(leasedToAnother ->
          scheduleReminderForEvent(jobProps, leasedToAnother, previousEventTimeMillis));
    }
  }

  /** @return {@link Optional} status for reminding, unless {@link LeaseAttemptStatus.NoLongerLeasingStatus} (hence nothing to do) */
  private Optional<LeaseAttemptStatus.LeasedToAnotherStatus> calcLeasedToAnotherStatusForReminder(LeaseAttemptStatus leaseAttempt) {
    if (leaseAttempt instanceof LeaseAttemptStatus.NoLongerLeasingStatus) { // all done: nothing to remind about
      return Optional.empty();
    } else if (leaseAttempt instanceof LeaseAttemptStatus.LeasedToAnotherStatus) { // already have one: just return it
      return Optional.of((LeaseAttemptStatus.LeasedToAnotherStatus) leaseAttempt);
    } else if (leaseAttempt instanceof LeaseAttemptStatus.LeaseObtainedStatus) { // remind w/o delay to immediately re-attempt handling
      return Optional.of(new LeaseAttemptStatus.LeasedToAnotherStatus(
          leaseAttempt.getConsensusLeaseParams(), 0L));
    } else {
      throw new RuntimeException("unexpected `LeaseAttemptStatus` derived type: '" + leaseAttempt.getClass().getName() + "' in '" + leaseAttempt + "'");
    }
  }

  /**
   * Called after obtaining a lease to both persist to the {@link DagActionStore} and
   * {@link MultiActiveLeaseArbiter#recordLeaseSuccess(LeaseAttemptStatus.LeaseObtainedStatus)}
   */
  private boolean persistLaunchDagAction(LeaseAttemptStatus.LeaseObtainedStatus leaseStatus) {
    DagActionStore.DagAction launchDagAction = leaseStatus.getConsensusDagAction();
    try {
      this.dagManagementStateStore.addDagAction(launchDagAction);
      this.numFlowsSubmitted.mark();
      // after successfully persisting, close the lease
      return this.multiActiveLeaseArbiter.recordLeaseSuccess(leaseStatus);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method is used by {@link FlowLaunchHandler#handleFlowLaunchTriggerEvent} to schedule a self-reminder to check
   * on the other participant's progress to finish acting on a dag action after the time the lease should expire.
   * @param jobProps
   * @param status used to extract event to be reminded for (stored in `consensusDagAction`) and the minimum time after
   *               which reminder should occur
   * @param triggerEventTimeMillis the event timestamp we were originally handling (only used for logging purposes)
   */
  private void scheduleReminderForEvent(Properties jobProps, LeaseAttemptStatus.LeasedToAnotherStatus status,
      long triggerEventTimeMillis) {
    DagActionStore.DagAction consensusDagAction = status.getConsensusDagAction();
    JobKey origJobKey = new JobKey(jobProps.getProperty(ConfigurationKeys.JOB_NAME_KEY, "<<no job name>>"),
        jobProps.getProperty(ConfigurationKeys.JOB_GROUP_KEY, "<<no job group>>"));
    try {
      if (!this.schedulerService.getScheduler().checkExists(origJobKey)) {
        log.warn("Skipping setting a reminder for a job that does not exist in the scheduler. Key: {}", origJobKey);
        this.jobDoesNotExistInSchedulerCount.inc();
        return;
      }
      Trigger reminderTrigger = createAndScheduleReminder(origJobKey, status, status.getEventTimeMillis());
      log.info("Flow Launch Handler - [{}, eventTimestamp: {}] - SCHEDULED REMINDER for event {} in {} millis",
          consensusDagAction, triggerEventTimeMillis, status.getEventTimeMillis(), reminderTrigger.getNextFireTime());
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
  protected Trigger createAndScheduleReminder(JobKey origJobKey, LeaseAttemptStatus.LeasedToAnotherStatus status,
      long triggerEventTimeMillis) throws SchedulerException {
    // Generate a suffix to differentiate the reminder Job and Trigger from the original JobKey and Trigger, so we can
    // allow us to keep track of additional properties needed for reminder events (all triggers associated with one job
    // refer to the same set of jobProperties)
    String reminderSuffix = createSuffixForJobTrigger(status);
    JobKey reminderJobKey = new JobKey(origJobKey.getName() + reminderSuffix, origJobKey.getGroup());
    JobDetailImpl jobDetail = createJobDetailForReminderEvent(origJobKey, status);
    jobDetail.setKey(reminderJobKey);
    Trigger reminderTrigger = JobScheduler.createTriggerForJob(reminderJobKey, getJobPropertiesFromJobDetail(jobDetail),
        Optional.of(reminderSuffix));
    log.debug("Flow Launch Handler - [{}, eventTimestamp: {}] -  attempting to schedule reminder for event {} with "
            + "reminderJobKey {} and reminderTriggerKey {}", status.getConsensusDagAction(), triggerEventTimeMillis,
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
  public static String createSuffixForJobTrigger(LeaseAttemptStatus.LeasedToAnotherStatus leasedToAnotherStatus) {
    return "reminder_for_" + leasedToAnotherStatus.getEventTimeMillis();
  }

  /**
   * Helper function used to extract JobDetail for job identified by the originalKey and update it be associated with
   * the event to revisit. It will update the jobKey to the reminderKey provides and the Properties map to
   * contain the cron scheduler for the reminder event and information about the event to revisit
   * @param originalKey
   * @param status
   * @return
   * @throws SchedulerException
   */
  protected JobDetailImpl createJobDetailForReminderEvent(JobKey originalKey, LeaseAttemptStatus.LeasedToAnotherStatus status)
      throws SchedulerException {
    // 1. shallow `.clone()` this top-level `JobDetailImpl`
    JobDetailImpl clonedJobDetail = (JobDetailImpl) this.schedulerService.getScheduler().getJobDetail(originalKey).clone();
    JobDataMap originalJobDataMap = clonedJobDetail.getJobDataMap();
    // 2. create a fresh `JobDataMap` specific to the reminder
    JobDataMap newJobDataMap = cloneAndUpdateJobProperties(originalJobDataMap, status, schedulerMaxBackoffMillis);
    // 3. update `clonedJobDetail` to point to the new `JobDataMap`
    clonedJobDetail.setJobDataMap(newJobDataMap);
    return clonedJobDetail;
  }

  public static Properties getJobPropertiesFromJobDetail(JobDetail jobDetail) {
    return (Properties) jobDetail.getJobDataMap().get(GobblinServiceJobScheduler.PROPERTIES_KEY);
  }

  /**
   * Adds the cronExpression, reminderTimestamp, originalEventTime values in the properties map of a new jobDataMap
   * cloned from the one provided and returns the new JobDataMap to the user.
   * `jobDataMap` and its `GobblinServiceJobScheduler.PROPERTIES_KEY` field are shallow, not deep-copied
   * @param jobDataMap
   * @param leasedToAnotherStatus
   * @param schedulerMaxBackoffMillis
   * @return
   */
  @VisibleForTesting
  public static JobDataMap cloneAndUpdateJobProperties(JobDataMap jobDataMap,
      LeaseAttemptStatus.LeasedToAnotherStatus leasedToAnotherStatus, int schedulerMaxBackoffMillis) {
    JobDataMap newJobDataMap = (JobDataMap) jobDataMap.clone();
    Properties newJobProperties =
        (Properties) ((Properties) jobDataMap.get(GobblinServiceJobScheduler.PROPERTIES_KEY)).clone();
    // Add a small randomization to the minimum reminder wait time to avoid 'thundering herd' issue
    long delayPeriodMillis = leasedToAnotherStatus.getMinimumLingerDurationMillis()
        + random.nextInt(schedulerMaxBackoffMillis);
    String cronExpression = createCronFromDelayPeriod(delayPeriodMillis);
    newJobProperties.put(ConfigurationKeys.JOB_SCHEDULE_KEY, cronExpression);
    // Saves the following properties in jobProps to retrieve when the trigger fires
    newJobProperties.put(ConfigurationKeys.SCHEDULER_EXPECTED_REMINDER_TIME_MILLIS_KEY,
        String.valueOf(getUTCTimeFromDelayPeriod(delayPeriodMillis)));
    // Use the db consensus timestamp for the reminder to ensure inter-host agreement. Participant trigger timestamps
    // can differ between participants and be interpreted as a reminder for a distinct flow trigger which will cause
    // excess flows to be triggered by the reminder functionality.
    newJobProperties.put(ConfigurationKeys.SCHEDULER_PRESERVED_CONSENSUS_EVENT_TIME_MILLIS_KEY,
        String.valueOf(leasedToAnotherStatus.getEventTimeMillis()));
    // Use this boolean to indicate whether this is a reminder event
    newJobProperties.put(ConfigurationKeys.FLOW_IS_REMINDER_EVENT_KEY, String.valueOf(true));
    // Replace reference to old Properties map with new cloned Properties
    newJobDataMap.put(GobblinServiceJobScheduler.PROPERTIES_KEY, newJobProperties);
    return newJobDataMap;
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
