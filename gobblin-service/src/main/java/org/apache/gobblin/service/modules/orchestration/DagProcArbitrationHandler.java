package org.apache.gobblin.service.modules.orchestration;


import java.util.Date;

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
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
  private final DagActionReminderScheduler dagActionReminderScheduler;

  @Inject
  public DagProcArbitrationHandler(Config config, Optional<MultiActiveLeaseArbiter> leaseDeterminationStore,
      SchedulerService schedulerService, Optional<DagActionStore> dagActionStore,
      DagActionReminderScheduler dagActionReminderScheduler) {
    super(config, leaseDeterminationStore, schedulerService, dagActionStore);
    // TODO: init scheduler in guice
    this.dagActionReminderScheduler = dagActionReminderScheduler;
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
  public void scheduleReminderForEvent(MultiActiveLeaseArbiter.LeasedToAnotherStatus leaseStatus, long triggerEventTimeMillis)
      throws SchedulerException {
    // TODO: determine which ts to use
    dagActionReminderScheduler.scheduleReminderJob(leaseStatus.getFlowAction(), leaseStatus.getEventTimeMillis());

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

  public static String createDagActionReminderKey(DagActionStore.DagAction dagAction) {
    return createDagActionReminderKey(dagAction.getFlowName(), dagAction.getFlowGroup(), dagAction.getJobName(),
        dagAction.getFlowExecutionId(), dagAction.getFlowActionType());
  }

  public static String createDagActionReminderKey(String flowName, String flowGroup, String jobName, String flowId,
      DagActionStore.FlowActionType flowActionType) {
    return String.format("%s.%s.%s.%s.%s", flowName, flowGroup, jobName, flowId, flowActionType);
  }

  public static JobDetail createReminderJobDetail(DagManagementTaskStreamImpl taskStream, DagActionStore.DagAction dagAction) {
    JobDataMap dataMap = new JobDataMap();
    dataMap.put(ReminderJob.DAG_TASK_STREAM, taskStream);
    dataMap.put(ConfigurationKeys.FLOW_NAME_KEY, dagAction.getFlowName());
    dataMap.put(ConfigurationKeys.FLOW_GROUP_KEY, dagAction.getFlowGroup());
    dataMap.put(ConfigurationKeys.JOB_NAME_KEY, dagAction.getJobName());
    dataMap.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagAction.getFlowExecutionId());
    dataMap.put(ReminderJob.FLOW_ACTION_TYPE_KEY, dagAction.getFlowActionType());

    return JobBuilder.newJob(ReminderJob.class)
        .withIdentity(createDagActionReminderKey(dagAction), dagAction.getFlowName())
        .usingJobData(dataMap)
        .build();
  }

  public static Trigger createReminderJobTrigger(DagActionStore.DagAction dagAction, long reminderDurationMillis) {
    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(createDagActionReminderKey(dagAction), dagAction.getFlowName())
        .startAt(new Date(System.currentTimeMillis() + reminderDurationMillis))
        .build();
    return trigger;
  }
}
