package org.apache.gobblin.service.modules.orchestration;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import org.apache.gobblin.runtime.api.DagActionStore;


// TODO: move to the right package & decide whether this wrapper is needed
public class DagActionReminderScheduler {
  /* The quartz scheduler used here should not use the job and trigger keys used by {@link DagProcArbitrationhandler} to
  avoid conflicts */
  private final org.quartz.Scheduler quartzScheduler;
  private final DagManagementTaskStreamImpl taskStream;

  public DagActionReminderScheduler(StdSchedulerFactory schedulerFactory, DagManagementTaskStreamImpl taskStream)
      throws SchedulerException {
    // Note: only one SchedulerFactory instance should exist per JVM
    // TODO: check whether this creates new scheduler
    this.quartzScheduler = schedulerFactory.getScheduler();
    this.taskStream = taskStream;
  }

  /**
   *  Uses a dagAction & reminder duration in milliseconds to create a reminder job that will fire
   *  `reminderDurationMillis` after the current time
   * @param dagAction
   * @param reminderDurationMillis
   * @throws SchedulerException
   */
  public void scheduleReminderJob(DagActionStore.DagAction dagAction, long reminderDurationMillis)
      throws SchedulerException {
    JobDetail jobDetail = DagProcArbitrationHandler.createReminderJobDetail(taskStream, dagAction);
    Trigger trigger = DagProcArbitrationHandler.createReminderJobTrigger(dagAction, reminderDurationMillis);
    quartzScheduler.scheduleJob(jobDetail, trigger);
  }

  public void unscheduleReminderJob(DagActionStore.DagAction dagAction) throws SchedulerException {
    JobDetail jobDetail = DagProcArbitrationHandler.createReminderJobDetail(taskStream, dagAction);
    quartzScheduler.deleteJob(jobDetail.getKey());
  }

}
