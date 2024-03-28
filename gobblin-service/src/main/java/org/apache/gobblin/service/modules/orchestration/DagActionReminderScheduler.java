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

import java.util.Optional;

import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import javax.inject.Inject;


/**
 * This class is used to keep track of reminders of pending flow action events to execute. A host calls the
 * {#scheduleReminderJob} on a flow action that it failed to acquire a lease on but has not yet completed. The reminder
 * will fire once the previous lease owner's lease is expected to expire.
 */
public class DagActionReminderScheduler {
  public static final String DAG_ACTION_REMINDER_SCHEDULER_KEY = "DagActionReminderScheduler";
  private final Scheduler quartzScheduler;
  private final Optional<DagManagement> dagManagement;

  @Inject
  public DagActionReminderScheduler(StdSchedulerFactory schedulerFactory, Optional<DagManagement> dagManagement)
      throws SchedulerException {
    // Create a new Scheduler to be used solely for the DagProc reminders
    this.quartzScheduler = schedulerFactory.getScheduler(DAG_ACTION_REMINDER_SCHEDULER_KEY);
    this.dagManagement = dagManagement;
  }

  /**
   *  Uses a dagAction & reminder duration in milliseconds to create a reminder job that will fire
   *  `reminderDurationMillis` after the current time
   * @param dagAction
   * @param reminderDurationMillis
   * @throws SchedulerException
   */
  public void scheduleReminder(DagActionStore.DagAction dagAction, long reminderDurationMillis)
      throws SchedulerException {
    if (!dagManagement.isPresent()) {
      throw new RuntimeException("DagManagement not initialized in multi-active execution mode when required.");
    }
    JobDetail jobDetail = ReminderSettingDagProcLeaseArbiter.createReminderJobDetail(dagManagement.get(), dagAction);
    Trigger trigger = ReminderSettingDagProcLeaseArbiter.createReminderJobTrigger(dagAction, reminderDurationMillis);
    quartzScheduler.scheduleJob(jobDetail, trigger);
  }

  public void unscheduleReminderJob(DagActionStore.DagAction dagAction) throws SchedulerException {
    if (!dagManagement.isPresent()) {
      throw new RuntimeException("DagManagement not initialized in multi-active execution mode when required.");
    }
    JobDetail jobDetail = ReminderSettingDagProcLeaseArbiter.createReminderJobDetail(dagManagement.get(), dagAction);
    quartzScheduler.deleteJob(jobDetail.getKey());
  }

}
