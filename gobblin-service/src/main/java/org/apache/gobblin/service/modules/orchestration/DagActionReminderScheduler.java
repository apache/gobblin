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
    // TODO: reuse std schedulerFactory to create new scheduler instance
    this.quartzScheduler = schedulerFactory.getScheduler();
    this.taskStream = taskStream;
  }

  // TODO: should only schedule a reminder if one doesn't exist -> maybe add an existence check
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
