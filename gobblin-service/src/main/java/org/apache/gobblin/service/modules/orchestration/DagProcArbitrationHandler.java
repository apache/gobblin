package org.apache.gobblin.service.modules.orchestration;


import org.quartz.DateBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import javax.inject.Inject;

import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;
import org.apache.gobblin.scheduler.SchedulerService;


// TODO: what additional methods are needed here?
public class DagProcArbitrationHandler extends GeneralLeaseArbitrationHandler {
  private final Scheduler scheduler;

  @Inject
  public DagProcArbitrationHandler(Config config, Optional<MultiActiveLeaseArbiter> leaseDeterminationStore,
      SchedulerService schedulerService, Optional<DagActionStore> dagActionStore, Scheduler scheduler) {
    super(config, leaseDeterminationStore, schedulerService, dagActionStore);
    // TODO: init scheduler in guice
    this.scheduler = scheduler;
  }

  /**
   * This method is used by the multi-active scheduler and multi-active execution classes (DagTaskStream) to attempt a
   * lease for a particular job event and return the status of the attempt.
   * @param flowAction
   * @param eventTimeMillis
   * @param isReminderEvent
   * @param skipFlowExecutionIdReplacement
   * @return
   */
  @Override
  public MultiActiveLeaseArbiter.LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction flowAction, long eventTimeMillis,
      boolean isReminderEvent, boolean skipFlowExecutionIdReplacement) {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * This method is used by the callers of the lease arbitration attempts over a dag action event to schedule a
   * self-reminder to check on the other participant's progress to finish acting on a dag action after the time the
   * lease should expire.
   * @param leaseStatus
   * @param triggerEventTimeMillis
   */
  public void scheduleReminderForEvent(MultiActiveLeaseArbiter.LeasedToAnotherStatus leaseStatus, long triggerEventTimeMillis) {
    throw new UnsupportedOperationException("Not supported");
    // TODO: how to add reminder for the dagAction to schedulerService
    // Define a job with a job name and group
    JobDetail job = JobBuilder.newJob(ReminderJob.class)
        .withIdentity("reminderJob", "reminderGroup")
        .build();

    // Define a trigger with a trigger name and group
    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity("reminderTrigger", "reminderGroup")
        .startAt(DateBuilder.futureDate(5, DateBuilder.IntervalUnit.MINUTE)) // Set the trigger to fire in 5 minutes
        .build();

    // Add properties to the trigger
    trigger.getJobDataMap().put("flowName", "myFlow");
    trigger.getJobDataMap().put("flowGroup", "myFlowGroup");
    trigger.getJobDataMap().put("flowId", "12345");
    trigger.getJobDataMap().put("jobName", "myJob");

    // Schedule the job with the trigger
    scheduler.scheduleJob(job, trigger);
  }

  public class ReminderJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

      // Get the properties from the trigger
      String flowName = context.getTrigger().getJobDataMap().getString("flowName");
      String flowGroup = context.getTrigger().getJobDataMap().getString("flowGroup");
      String flowId = context.getTrigger().getJobDataMap().getString("flowId");
      String jobName = context.getTrigger().getJobDataMap().getString("jobName");

      // Do something with the properties
      System.out.println("Reminder for job " + jobName + " in flow " + flowName + " (" + flowGroup + ") with ID " + flowId);

    }

  }
}
