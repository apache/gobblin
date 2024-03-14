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

import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.typesafe.config.Config;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.MultiActiveLeaseArbiter;


/**
 * Decorator used to coordinate multiple hosts with execution components enabled to respond to flow action events with
 * added capabilities to properly handle the result of attempted ownership over these flow action events. It uses the
 * {@link MultiActiveLeaseArbiter} to determine a single lease owner at a given event time for a flow action event.
 * After acquiring the lease, the host can pursue executing the action. Once it has completed this action, it
 * marks the lease as completed by calling the
 * {@link MultiActiveLeaseArbiter#recordLeaseSuccess(MultiActiveLeaseArbiter.LeaseObtainedStatus)} method. Hosts
 * that fail to acquire a lease will use the {@link DagActionReminderScheduler} to set a reminder for the flow action
 * event to check back in on the previous lease owner's completion status.
 */
@Slf4j
public class ReminderSettingDagProcLeaseArbiter implements MultiActiveLeaseArbiter {
  private final MultiActiveLeaseArbiter decoratedLeaseArbiter;
  private final DagActionReminderScheduler _dagActionReminderScheduler;
  private final Config config;

  @Inject
  public ReminderSettingDagProcLeaseArbiter(Config config, MultiActiveLeaseArbiter leaseArbiter, DagActionReminderScheduler dagActionReminderScheduler) throws IOException {
    this.decoratedLeaseArbiter = leaseArbiter;
    this._dagActionReminderScheduler = dagActionReminderScheduler;
    this.config = config;
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
    try {
      MultiActiveLeaseArbiter.LeaseAttemptStatus leaseAttemptStatus = this.tryAcquireLease(flowAction, eventTimeMillis, isReminderEvent, skipFlowExecutionIdReplacement);
      /* Schedule a reminder for the event unless the lease has been completed to safeguard against case lease owner
      fails to complete lease
      */
      if (leaseAttemptStatus instanceof MultiActiveLeaseArbiter.LeaseObtainedStatus) {
        scheduleReminderForEvent((LeaseObtainedStatus) leaseAttemptStatus);
      } else if (leaseAttemptStatus instanceof  MultiActiveLeaseArbiter.LeasedToAnotherStatus) {
        scheduleReminderForEvent((LeasedToAnotherStatus) leaseAttemptStatus);
      }
      return leaseAttemptStatus;
    } catch (SchedulerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean recordLeaseSuccess(LeaseObtainedStatus status)
      throws IOException {
    return this.decoratedLeaseArbiter.recordLeaseSuccess(status);
  }

  protected void scheduleReminderForEvent(MultiActiveLeaseArbiter.LeasedToAnotherStatus leaseStatus)
      throws SchedulerException {
    _dagActionReminderScheduler.scheduleReminder(leaseStatus.getDagAction(), leaseStatus.getMinimumLingerDurationMillis());
  }

  protected void scheduleReminderForEvent(MultiActiveLeaseArbiter.LeaseObtainedStatus leaseStatus)
      throws SchedulerException {
    _dagActionReminderScheduler.scheduleReminder(leaseStatus.getDagAction(), leaseStatus.getMinimumLingerDurationMillis());
  }

  @Slf4j
  public static class ReminderJob implements Job {
    public static final String FLOW_ACTION_TYPE_KEY = "flow.actionType";
    public static final String DAG_MANAGEMENT_KEY = "dag.management";

    @Override
    public void execute(JobExecutionContext context) {
      // Get properties from the trigger to create a dagAction
      JobDataMap jobDataMap = context.getTrigger().getJobDataMap();
      String flowName = jobDataMap.getString(ConfigurationKeys.FLOW_NAME_KEY);
      String flowGroup = jobDataMap.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String jobName = jobDataMap.getString(ConfigurationKeys.JOB_NAME_KEY);
      String flowId = jobDataMap.getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY);
      DagActionStore.DagActionType dagActionType = DagActionStore.DagActionType.valueOf(
          jobDataMap.getString(FLOW_ACTION_TYPE_KEY));
      DagManagement dagManagement = (DagManagement) jobDataMap.get(DAG_MANAGEMENT_KEY);

      log.info("DagProc reminder triggered for (flowGroup: " + flowGroup + ", flowName: " + flowName
          + ", flowExecutionId: " + flowId + ", jobName: " + jobName +")");

      DagActionStore.DagAction dagAction = new DagActionStore.DagAction(flowGroup, flowName, flowId, jobName,
          dagActionType);

      try {
        dagManagement.addDagAction(dagAction);
      } catch (IOException e) {
        log.error("Failed to add DagAction to DagManagement. Action: {}", dagAction);
      }
    }
  }

  public static String createDagActionReminderKey(DagActionStore.DagAction dagAction) {
    return createDagActionReminderKey(dagAction.getFlowName(), dagAction.getFlowGroup(), dagAction.getJobName(),
        dagAction.getFlowExecutionId(), dagAction.get_dagActionType());
  }

  public static String createDagActionReminderKey(String flowName, String flowGroup, String jobName, String flowId,
      DagActionStore.DagActionType dagActionType) {
    return String.format("%s.%s.%s.%s.%s", flowGroup, flowName, flowId, jobName, dagActionType);
  }

  public static JobDetail createReminderJobDetail(DagManagement dagManagement, DagActionStore.DagAction dagAction) {
    JobDataMap dataMap = new JobDataMap();
    dataMap.put(ReminderJob.DAG_MANAGEMENT_KEY, dagManagement);
    dataMap.put(ConfigurationKeys.FLOW_NAME_KEY, dagAction.getFlowName());
    dataMap.put(ConfigurationKeys.FLOW_GROUP_KEY, dagAction.getFlowGroup());
    dataMap.put(ConfigurationKeys.JOB_NAME_KEY, dagAction.getJobName());
    dataMap.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagAction.getFlowExecutionId());
    dataMap.put(ReminderJob.FLOW_ACTION_TYPE_KEY, dagAction.get_dagActionType());

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
