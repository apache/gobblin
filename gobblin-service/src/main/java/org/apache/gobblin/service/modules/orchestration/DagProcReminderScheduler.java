package org.apache.gobblin.service.modules.orchestration;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import javax.inject.Inject;

import org.apache.gobblin.runtime.api.DagActionStore;


/**
 * This class is used to keep track of reminders of pending flow action events to execute. A host calls the
 * {#scheduleReminderJob} on a flow action that it failed to acquire a lease on but has not yet completed. The reminder
 * will fire once the previous lease owner's lease is expected to expire.
 */
public class DagProcReminderScheduler {
  private final Scheduler quartzScheduler;
  private final DagManagementTaskStreamImpl taskStream;

  @Inject
  public DagProcReminderScheduler(StdSchedulerFactory schedulerFactory, DagManagementTaskStreamImpl taskStream)
      throws SchedulerException {
    // Create a new Scheduler to be used solely for the DagProc reminders
    this.quartzScheduler = schedulerFactory.getScheduler("DagProcScheduler");
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
    JobDetail jobDetail = DagProcArbiterDecorator.createReminderJobDetail(taskStream, dagAction);
    Trigger trigger = DagProcArbiterDecorator.createReminderJobTrigger(dagAction, reminderDurationMillis);
    quartzScheduler.scheduleJob(jobDetail, trigger);
  }

  public void unscheduleReminderJob(DagActionStore.DagAction dagAction) throws SchedulerException {
    JobDetail jobDetail = DagProcArbiterDecorator.createReminderJobDetail(taskStream, dagAction);
    quartzScheduler.deleteJob(jobDetail.getKey());
  }

}
