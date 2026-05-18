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
import java.util.Date;
import java.util.Properties;
import java.util.function.Supplier;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import com.google.common.annotations.VisibleForTesting;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;


/**
 * This class is used to keep track of reminders of pending flow action events to execute. A host calls the
 * {#scheduleReminderJob} on a flow action that it failed to acquire a lease on but has not yet completed. The reminder
 * will fire once the previous lease owner's lease is expected to expire.
 * There are two type of reminders, i) Deadline reminders, that are created while processing deadline
 * {@link org.apache.gobblin.service.modules.orchestration.DagActionStore.DagActionType#ENFORCE_FLOW_FINISH_DEADLINE} and
 * {@link org.apache.gobblin.service.modules.orchestration.DagActionStore.DagActionType#ENFORCE_JOB_START_DEADLINE} when
 * they set reminder for the duration equals for the "deadline time", and ii) Retry reminders, that are created to retry
 * the processing of any dag action in case the first attempt by other lease owner fails.
 * Note that deadline dag actions first create `Deadline reminders` and then `Retry reminders` in their life-cycle, while
 * other dag actions only create `Retry reminders`.
 */
@Slf4j
@Singleton
public class DagActionReminderScheduler {
  public static final String DAG_ACTION_REMINDER_SCHEDULER_NAME = "DagActionReminderScheduler";
  public static final String RetryReminderKeyGroup = "RetryReminder";
  public static final String DeadlineReminderKeyGroup = "DeadlineReminder";
  @VisibleForTesting
  final Scheduler quartzScheduler;
  private final DagManagement dagManagement;

  @Inject
  public DagActionReminderScheduler(DagManagement dagManagement) throws SchedulerException {
    // Creates a new Scheduler to be used solely for the DagProc reminders
    this.quartzScheduler = createScheduler();
    this.quartzScheduler.start();
    this.quartzScheduler.setJobFactory(new ReminderJobFactory());
    this.dagManagement = dagManagement;
  }

  private Scheduler createScheduler() throws SchedulerException {
    Properties properties = new Properties();
    properties.setProperty("org.quartz.scheduler.instanceName", DAG_ACTION_REMINDER_SCHEDULER_NAME);
    properties.setProperty("org.quartz.threadPool.threadCount", "10");
    return new StdSchedulerFactory(properties).getScheduler();
  }

  /**
   *  Uses a dagAction & reminder duration in milliseconds to create a reminder job that will fire
   *  `reminderDurationMillis` after the current time
   * @param leaseParams
   * @param reminderDurationMillis
   * @throws SchedulerException
   */
  public void scheduleReminder(DagActionStore.LeaseParams leaseParams, long reminderDurationMillis,
      boolean isDeadlineReminder) throws SchedulerException {
    DagActionStore.DagAction dagAction = leaseParams.getDagAction();
    JobDetail jobDetail = createReminderJobDetail(leaseParams, isDeadlineReminder);
    Trigger trigger = createReminderJobTrigger(leaseParams, reminderDurationMillis, System::currentTimeMillis, isDeadlineReminder);
    // Deadline reminders are keyed only by the dagAction's primary key (no event time). The DagActionStore enforces
    // at most one row per that key, so any pre-existing deadline reminder with the same key represents a stale entry
    // from the duplicate-insert path (delete+reinsert in DagProcUtils.sendEnforce*DeadlineDagAction). If the change
    // events arrive out of order on the consumer, the new INSERT could otherwise collide with the not-yet-unscheduled
    // old reminder and throw ObjectAlreadyExistsException, causing the new deadline to be lost. Replace semantics
    // are safe here because the newly-scheduled reminder is authoritative for that dagAction tuple.
    if (isDeadlineReminder && quartzScheduler.checkExists(jobDetail.getKey())) {
      log.info("Replacing pre-existing deadline reminder for {}", dagAction);
      quartzScheduler.deleteJob(jobDetail.getKey());
    }
    log.info("Setting reminder for {} in {} ms, isDeadlineTrigger: {}", dagAction, reminderDurationMillis, isDeadlineReminder);
    quartzScheduler.scheduleJob(jobDetail, trigger);
  }

  public void unscheduleReminderJob(DagActionStore.LeaseParams leaseParams, boolean isDeadlineTrigger) throws SchedulerException {
    log.info("Unsetting reminder for {}, isDeadlineTrigger: {}", leaseParams, isDeadlineTrigger);
    if (!quartzScheduler.deleteJob(createJobKey(leaseParams, isDeadlineTrigger))) {
      log.warn("Reminder not found for {}. Possibly the event is received out-of-order.", leaseParams);
    }
  }

  /**
   * Unschedule a deadline reminder identified solely by its {@link DagActionStore.DagAction}. The deadline reminder
   * key intentionally omits the lease event time (see {@link #createDagActionReminderKey}), so callers that only
   * have the dagAction (e.g. a DagActionStore DELETE event, whose payload has no event time) can still remove the
   * reminder.
   */
  public void unscheduleReminderJob(DagActionStore.DagAction dagAction) throws SchedulerException {
    log.info("Unsetting deadline reminder for {}", dagAction);
    if (!quartzScheduler.deleteJob(new JobKey(createDeadlineReminderKey(dagAction), DeadlineReminderKeyGroup))) {
      // Expected on the normal lifecycle: the reminder fires at the deadline, the enforce-deadline task cleans up
      // via deleteDagAction, and by the time the resulting DELETE event reaches this handler the Quartz job is
      // already gone. Also benign on cold-start / partition-rebalance, where the reminder was scheduled on a
      // different host. Logged at debug to avoid flooding warn-level operator views.
      log.debug("No deadline reminder to unschedule for {} (reminder already fired, or scheduled on a different host).",
          dagAction);
    }
  }

  /**
   * Creates a key for the reminder job by concatenating all dagAction fields and, for retry reminders only, the
   * lease event time of the dagAction.
   * <p>
   * For retry reminders the event time is required to ensure unique keys for multiple instances of the same action
   * on the same flow execution that originate more than 'epsilon' apart (applicable to KILL and RESUME).
   * {@link MultiActiveLeaseArbiter} uses the event time to distinguish these distinct occurrences. Without it,
   * subsequent reminders for the same action would fail to insert because the Quartz key already exists.
   * <p>
   * For deadline reminders ({@code ENFORCE_JOB_START_DEADLINE}, {@code ENFORCE_FLOW_FINISH_DEADLINE}) the event time
   * is omitted. The DagActionStore enforces a primary key over
   * {@code (flowGroup, flowName, flowExecutionId, jobName, dagActionType)} (see
   * {@code DagProcUtils.sendEnforce*DeadlineDagAction}), so at most one deadline reminder exists for that tuple at
   * any time. Omitting the event time lets the change monitor unschedule the reminder when the dagAction row is
   * deleted, without needing the original event time (which is not present in the DELETE event payload).
   */
  public static String createDagActionReminderKey(DagActionStore.LeaseParams leaseParams, boolean isDeadlineReminder) {
    DagActionStore.DagAction dagAction = leaseParams.getDagAction();
    if (isDeadlineReminder) {
      return createDeadlineReminderKey(dagAction);
    }
    return String.join(".",
        dagAction.getFlowGroup(),
        dagAction.getFlowName(),
        String.valueOf(dagAction.getFlowExecutionId()),
        dagAction.getJobName(),
        String.valueOf(dagAction.getDagActionType()),
        String.valueOf(leaseParams.getEventTimeMillis()));
  }

  private static String createDeadlineReminderKey(DagActionStore.DagAction dagAction) {
    return String.join(".",
        dagAction.getFlowGroup(),
        dagAction.getFlowName(),
        String.valueOf(dagAction.getFlowExecutionId()),
        dagAction.getJobName(),
        String.valueOf(dagAction.getDagActionType()));
  }

  /**
   * Creates a JobKey object for the reminder job where the name is the DagActionReminderKey from above and the group is
   * the flowGroup
   */
  public static JobKey createJobKey(DagActionStore.LeaseParams leaseParams, boolean isDeadlineReminder) {
    return new JobKey(createDagActionReminderKey(leaseParams, isDeadlineReminder),
        isDeadlineReminder ? DeadlineReminderKeyGroup : RetryReminderKeyGroup);
  }

  private static TriggerKey createTriggerKey(DagActionStore.LeaseParams leaseParams, boolean isDeadlineReminder) {
    return new TriggerKey(createDagActionReminderKey(leaseParams, isDeadlineReminder),
        isDeadlineReminder ? DeadlineReminderKeyGroup : RetryReminderKeyGroup);
  }

  /**
   * Creates a jobDetail containing flow and job identifying information in the jobDataMap, uniquely identified
   *  by a key comprised of the dagAction's fields. boolean isDeadlineReminder is flag that tells if this createReminder
   *  requests are for deadline dag actions that are setting reminder for deadline duration.
   */
  public static JobDetail createReminderJobDetail(DagActionStore.LeaseParams leaseParams, boolean isDeadlineReminder) {
    JobDataMap dataMap = new JobDataMap();
    DagActionStore.DagAction dagAction = leaseParams.getDagAction();
    dataMap.put(ConfigurationKeys.FLOW_NAME_KEY, dagAction.getFlowName());
    dataMap.put(ConfigurationKeys.FLOW_GROUP_KEY, dagAction.getFlowGroup());
    dataMap.put(ConfigurationKeys.JOB_NAME_KEY, dagAction.getJobName());
    dataMap.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagAction.getFlowExecutionId());
    dataMap.put(ReminderJob.FLOW_ACTION_TYPE_KEY, dagAction.getDagActionType());
    dataMap.put(ReminderJob.FLOW_ACTION_EVENT_TIME_KEY, leaseParams.getEventTimeMillis());
    // Carry the source DagActionStore row-insert time through the Quartz reminder so a host-failure-driven
    // reattempt still preserves the original timestamp for end-to-end latency instrumentation.
    dataMap.put(ReminderJob.FLOW_ACTION_STORE_INSERT_TIME_MILLIS_KEY, leaseParams.getStoreInsertTimeMillis());

    return JobBuilder.newJob(ReminderJob.class)
        .withIdentity(createJobKey(leaseParams, isDeadlineReminder))
        .usingJobData(dataMap)
        .build();
  }

  /**
   * Creates a Trigger object with the same key as the ReminderJob (since only one trigger is expected to be associated
   * with a job at any given time) that should fire after `reminderDurationMillis` millis. It uses
   * `getCurrentTimeMillis` to determine the current time.
   */
  public static Trigger createReminderJobTrigger(DagActionStore.LeaseParams leaseParams, long reminderDurationMillis,
      Supplier<Long> getCurrentTimeMillis, boolean isDeadlineReminder) {
    return TriggerBuilder.newTrigger()
        .withIdentity(createTriggerKey(leaseParams, isDeadlineReminder))
        .startAt(new Date(getCurrentTimeMillis.get() + reminderDurationMillis))
        .build();
  }

  public class ReminderJobFactory implements JobFactory {
    @Override
    public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) {
      return new ReminderJob(dagManagement);
    }
  }

  /**
   * These jobs are scheduled and used by the {@link DagActionReminderScheduler}.
   * When the reminder deadline is completed, these jobs are invoked by Quartz scheduler.
   * They create a {@link DagActionStore.LeaseParams} and forward them to {@link DagManagement} for further processing.
   */
  @RequiredArgsConstructor
  public static class ReminderJob implements Job {
    public static final String FLOW_ACTION_TYPE_KEY = "flow.actionType";
    public static final String FLOW_ACTION_EVENT_TIME_KEY = "flow.eventTime";
    public static final String FLOW_ACTION_STORE_INSERT_TIME_MILLIS_KEY = "flow.storeInsertTimeMillis";
    private final DagManagement dagManagement;

    @Override
    public void execute(JobExecutionContext context) {
      // Get properties from the trigger to create a dagAction
      JobDataMap jobDataMap = context.getMergedJobDataMap();
      String flowName = jobDataMap.getString(ConfigurationKeys.FLOW_NAME_KEY);
      String flowGroup = jobDataMap.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String jobName = jobDataMap.getString(ConfigurationKeys.JOB_NAME_KEY);
      long flowExecutionId = jobDataMap.getLong(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
      DagActionStore.DagActionType dagActionType = (DagActionStore.DagActionType) jobDataMap.get(FLOW_ACTION_TYPE_KEY);
      long eventTimeMillis = jobDataMap.getLong(FLOW_ACTION_EVENT_TIME_KEY);
      // Restore the original DagActionStore row-insert time so downstream LaunchDagProc can still stamp
      // the JobSpec on a reminder-driven reattempt. Defaults to UNKNOWN for reminders scheduled by older
      // code paths that did not populate the key.
      long storeInsertTimeMillis = jobDataMap.containsKey(FLOW_ACTION_STORE_INSERT_TIME_MILLIS_KEY)
          ? jobDataMap.getLong(FLOW_ACTION_STORE_INSERT_TIME_MILLIS_KEY)
          : DagActionStore.LeaseParams.UNKNOWN_STORE_INSERT_TIME_MILLIS;

      DagActionStore.LeaseParams reminderLeaseParams = new DagActionStore.LeaseParams(
          new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, dagActionType),
          true, eventTimeMillis, storeInsertTimeMillis);
      log.info("DagProc reminder triggered for dagAction event: {}", reminderLeaseParams);

      try {
        dagManagement.addDagAction(reminderLeaseParams);
      } catch (IOException e) {
        log.error("Failed to add DagAction event to DagManagement. dagAction event: {}", reminderLeaseParams);
      }
    }
  }
}
