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
import org.quartz.impl.StdSchedulerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.core.GobblinServiceManager;


/**
 * This class is used to keep track of reminders of pending flow action events to execute. A host calls the
 * {#scheduleReminderJob} on a flow action that it failed to acquire a lease on but has not yet completed. The reminder
 * will fire once the previous lease owner's lease is expected to expire.
 */
@Singleton
public class DagActionReminderScheduler {
  public static final String DAG_ACTION_REMINDER_SCHEDULER_KEY = "DagActionReminderScheduler";
  private final Scheduler quartzScheduler;

  @Inject
  public DagActionReminderScheduler(StdSchedulerFactory schedulerFactory)
      throws SchedulerException {
    // Creates a new Scheduler to be used solely for the DagProc reminders
    this.quartzScheduler = schedulerFactory.getScheduler();
  }

  /**
   *  Uses a dagAction & reminder duration in milliseconds to create a reminder job that will fire
   *  `reminderDurationMillis` after the current time
   * @param dagActionLeaseObject
   * @param reminderDurationMillis
   * @throws SchedulerException
   */
  public void scheduleReminder(DagActionStore.DagActionLeaseObject dagActionLeaseObject, long reminderDurationMillis)
      throws SchedulerException {
    JobDetail jobDetail = createReminderJobDetail(dagActionLeaseObject);
    Trigger trigger = createReminderJobTrigger(dagActionLeaseObject.getDagAction(), reminderDurationMillis,
        System::currentTimeMillis);
    quartzScheduler.scheduleJob(jobDetail, trigger);
  }

  public void unscheduleReminderJob(DagActionStore.DagAction dagAction) throws SchedulerException {
    quartzScheduler.deleteJob(createJobKey(dagAction));
  }

  /**
   * Static class used to store information regarding a pending dagAction that needs to be revisited at a later time
   * by {@link DagManagement} interface to re-attempt a lease on if it has not been completed by the previous owner.
   * These jobs are scheduled and used by the {@link DagActionReminderScheduler}.
   */
  @Slf4j
  public static class ReminderJob implements Job {
    public static final String FLOW_ACTION_TYPE_KEY = "flow.actionType";
    public static final String FLOW_ACTION_EVENT_TIME_KEY = "flow.eventTime";

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

      DagActionStore.DagActionLeaseObject reminderDagActionLeaseObject = new DagActionStore.DagActionLeaseObject(
          new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName, dagActionType),
          true, eventTimeMillis);
      log.info("DagProc reminder triggered for dagAction event: {}", reminderDagActionLeaseObject);

      try {
        DagManagement dagManagement = GobblinServiceManager.getClass(DagManagement.class);
        dagManagement.addReminderDagAction(reminderDagActionLeaseObject);
      } catch (IOException e) {
        log.error("Failed to add DagAction event to DagManagement. dagAction event: {}", reminderDagActionLeaseObject);
      }
    }
  }

  /**
   * Creates a key for the reminder job by concatenating all dagAction fields
   */
  public static String createDagActionReminderKey(DagActionStore.DagAction dagAction) {
    return String.format("%s.%s.%s.%s.%s", dagAction.getFlowGroup(), dagAction.getFlowName(),
        dagAction.getFlowExecutionId(), dagAction.getJobName(), dagAction.getDagActionType());
  }

  /**
   * Creates a JobKey object for the reminder job where the name is the DagActionReminderKey from above and the group is
   * the flowGroup
   */
  public static JobKey createJobKey(DagActionStore.DagAction dagAction) {
    return new JobKey(createDagActionReminderKey(dagAction), dagAction.getFlowGroup());
  }

  /**
   * Creates a jobDetail containing flow and job identifying information in the jobDataMap, uniquely identified
   *  by a key comprised of the dagAction's fields.
   */
  public static JobDetail createReminderJobDetail(DagActionStore.DagActionLeaseObject dagActionLeaseObject) {
    JobDataMap dataMap = new JobDataMap();
    dataMap.put(ConfigurationKeys.FLOW_NAME_KEY, dagActionLeaseObject.getDagAction().getFlowName());
    dataMap.put(ConfigurationKeys.FLOW_GROUP_KEY, dagActionLeaseObject.getDagAction().getFlowGroup());
    dataMap.put(ConfigurationKeys.JOB_NAME_KEY, dagActionLeaseObject.getDagAction().getJobName());
    dataMap.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagActionLeaseObject.getDagAction().getFlowExecutionId());
    dataMap.put(ReminderJob.FLOW_ACTION_TYPE_KEY, dagActionLeaseObject.getDagAction().getDagActionType());
    dataMap.put(ReminderJob.FLOW_ACTION_EVENT_TIME_KEY, dagActionLeaseObject.getEventTimeMillis());

    return JobBuilder.newJob(ReminderJob.class)
        .withIdentity(createDagActionReminderKey(dagActionLeaseObject.getDagAction()),
            dagActionLeaseObject.getDagAction().getFlowGroup())
        .usingJobData(dataMap)
        .build();
  }

  /**
   * Creates a Trigger object with the same key as the ReminderJob (since only one trigger is expected to be associated
   * with a job at any given time) that should fire after `reminderDurationMillis` millis. It uses
   * `getCurrentTimeMillis` to determine the current time.
   */
  public static Trigger createReminderJobTrigger(DagActionStore.DagAction dagAction, long reminderDurationMillis,
      Supplier<Long> getCurrentTimeMillis) {
    return TriggerBuilder.newTrigger()
        .withIdentity(createDagActionReminderKey(dagAction), dagAction.getFlowGroup())
        .startAt(new Date(getCurrentTimeMillis.get() + reminderDurationMillis))
        .build();
  }
}
