package org.apache.gobblin.service.modules.orchestration;

import org.quartz.*;


// TODO: move to the right package & decide whether this wrapper is needed
public class DagActionReminderScheduler {
  private final org.quartz.Scheduler quartzScheduler;
  private final DagManagementTaskStreamImpl taskStream;

  public DagActionReminderScheduler(org.quartz.Scheduler quartzScheduler, DagManagementTaskStreamImpl taskStream) {
    this.quartzScheduler = quartzScheduler;
    this.taskStream = taskStream;
  }

  public void scheduleReminderJob(String flowName, String flowGroup, String jobName, String flowId, String flowActionType, long reminderDurationMillis) throws SchedulerException {
    JobDetail jobDetail = DagProcArbitrationHandler.createReminderJobDetail(taskStream, flowName, flowGroup, jobName, flowId, flowActionType);

    Trigger trigger = DagProcArbitrationHandler.createReminderJobTrigger(flowName, flowGroup, jobName, flowId, flowActionType, reminderDurationMillis);

    quartzScheduler.scheduleJob(jobDetail, trigger);
  }

}
