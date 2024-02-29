package org.apache.gobblin.service.modules.orchestration;


import java.util.Date;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
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

    // Schedule the job with the trigger
    scheduler.scheduleJob(job, trigger);
  }

  @Slf4j
  public class ReminderJob implements Job {
    public static final String FLOW_ACTION_TYPE_KEY = "flow.actionType";
    public static final String DAG_TASK_STREAM = "dag.taskStream";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
      // Get properties from the trigger to create a dagAction
      String flowName = context.getTrigger().getJobDataMap().getString(ConfigurationKeys.FLOW_NAME_KEY);
      String flowGroup = context.getTrigger().getJobDataMap().getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String jobName = context.getTrigger().getJobDataMap().getString(ConfigurationKeys.JOB_NAME_KEY);
      String flowId = context.getTrigger().getJobDataMap().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
      DagActionStore.FlowActionType flowActionType = DagActionStore.FlowActionType.valueOf(
          context.getTrigger().getJobDataMap().getString(FLOW_ACTION_TYPE_KEY));
      DagManagementTaskStreamImpl dagManagementTaskStream =
          (DagManagementTaskStreamImpl) context.getTrigger().getJobDataMap().get(DAG_TASK_STREAM);

      // TODO: add meaningful log statement
      log.info("Reminder for job " + jobName + " in flow " + flowName + " (" + flowGroup + ") with ID " + flowId);

      DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowId, jobName, flowActionType);

      dagManagementTaskStream.addDagAction(dagAction);
    }
  }

  public static String createDagActionReminderKey(String flowName, String flowGroup, String jobName, String flowId, String flowActionType) {
    return String.format("%s.%s.%s.%s.%s", flowName, flowGroup, jobName, flowId, flowActionType);
  }

  public static JobDetail createReminderJobDetail(DagManagementTaskStreamImpl taskStream, String flowName, String flowGroup,
      String jobName, String flowExecutionId, String flowActionType) {
    JobDataMap dataMap = new JobDataMap();
    dataMap.put(ReminderJob.DAG_TASK_STREAM, taskStream);
    dataMap.put(ConfigurationKeys.FLOW_NAME_KEY, flowName);
    dataMap.put(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup);
    dataMap.put(ConfigurationKeys.JOB_NAME_KEY, jobName);
    dataMap.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
    dataMap.put(ReminderJob.FLOW_ACTION_TYPE_KEY, flowActionType);

    return JobBuilder.newJob(ReminderJob.class)
        .withIdentity(createDagActionReminderKey(flowName, flowGroup, jobName, flowExecutionId, flowActionType), flowName)
        .usingJobData(dataMap)
        .build();
  }

  public static Trigger createReminderJobTrigger(String flowName, String flowGroup, String jobName,
      String flowExecutionId, String flowActionType, long reminderDurationMillis) {
    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(createDagActionReminderKey(flowName, flowGroup, jobName, flowExecutionId, flowActionType), flowName)
        .startAt(new Date(System.currentTimeMillis() + reminderDurationMillis))
        .build();
    return trigger;
  }
}
