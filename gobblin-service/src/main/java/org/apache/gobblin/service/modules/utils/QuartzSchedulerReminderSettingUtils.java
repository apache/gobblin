package org.apache.gobblin.service.modules.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.impl.JobDetailImpl;

import com.google.common.annotations.VisibleForTesting;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;

// TODO pass in other params to get this to compile
public class QuartzSchedulerReminderSettingUtils {
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
