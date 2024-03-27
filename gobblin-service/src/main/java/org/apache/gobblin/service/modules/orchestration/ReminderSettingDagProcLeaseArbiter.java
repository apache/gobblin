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
import java.util.Optional;

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
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.modules.core.GobblinServiceGuiceModule;


/**
 * Decorator used to coordinate multiple hosts with execution components enabled to respond to flow action events with
 * added capabilities to properly handle the result of attempted ownership over these flow action events. It uses the
 * {@link MultiActiveLeaseArbiter} to determine a single lease owner at a given event time for a flow action event.
 * If the status of the lease ownership attempt is anything other than an indication the lease has been completed
 * ({@link LeaseAttemptStatus.NoLongerLeasingStatus}) then the
 * {@link MultiActiveLeaseArbiter#tryAcquireLease} method will set a reminder for the flow action using
 * {@link DagActionReminderScheduler} to reattempt the lease after the current lease holder's grant would have expired.
 */
@Slf4j
public class ReminderSettingDagProcLeaseArbiter implements MultiActiveLeaseArbiter {
  private final Optional<MultiActiveLeaseArbiter> decoratedLeaseArbiter;
  private final Optional<DagActionReminderScheduler> dagActionReminderScheduler;
  private final Config config;
  private final String MISSING_OPTIONAL_ERROR_MESSAGE = String.format("Multi-active execution is not enabled so dag "
      + "action should not passed to %s", ReminderSettingDagProcLeaseArbiter.class.getSimpleName());

  @Inject
  public ReminderSettingDagProcLeaseArbiter(Config config,
      @Named(GobblinServiceGuiceModule.EXECUTOR_LEASE_ARBITER_NAME) Optional<MultiActiveLeaseArbiter> leaseArbiter,
      Optional<DagActionReminderScheduler> dagActionReminderScheduler) {
    this.decoratedLeaseArbiter = leaseArbiter;
    this.dagActionReminderScheduler = dagActionReminderScheduler;
    this.config = config;
  }

  /**
   * Attempts a lease for a particular job event and sets a reminder to revisit if the lease has not been completed.
   */
  @Override
  public LeaseAttemptStatus tryAcquireLease(DagActionStore.DagAction dagAction, long eventTimeMillis,
      boolean isReminderEvent, boolean skipFlowExecutionIdReplacement) {
    if (this.decoratedLeaseArbiter.isPresent()) {
      try {
        LeaseAttemptStatus leaseAttemptStatus =
            this.decoratedLeaseArbiter.get().tryAcquireLease(dagAction, eventTimeMillis, isReminderEvent,
                skipFlowExecutionIdReplacement);
      /* Schedule a reminder for the event unless the lease has been completed to safeguard against the case where even
      we, when we might become the lease owner still fail to complete processing
      */
        if (!(leaseAttemptStatus instanceof LeaseAttemptStatus.NoLongerLeasingStatus)) {
          scheduleReminderForEvent(leaseAttemptStatus);
        }
        return leaseAttemptStatus;
      } catch (SchedulerException | IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException(MISSING_OPTIONAL_ERROR_MESSAGE);
    }
  }

  @Override
  public boolean recordLeaseSuccess(LeaseAttemptStatus.LeaseObtainedStatus status)
      throws IOException {
    if (!this.decoratedLeaseArbiter.isPresent()) {
      throw new RuntimeException(MISSING_OPTIONAL_ERROR_MESSAGE);
    }
    return this.decoratedLeaseArbiter.get().recordLeaseSuccess(status);
  }

  protected void scheduleReminderForEvent(LeaseAttemptStatus leaseStatus)
      throws SchedulerException {
    if (!this.dagActionReminderScheduler.isPresent()) {
      throw new RuntimeException(MISSING_OPTIONAL_ERROR_MESSAGE);
    }
    dagActionReminderScheduler.get().scheduleReminder(leaseStatus.getDagAction(),
        leaseStatus.getMinimumLingerDurationMillis());
  }

  /**
   * Static class used to store information regarding a pending dagAction that needs to be revisited at a later time
   * by {@link DagManagement} interface to re-attempt a lease on if it has not been completed by the previous owner.
   * These jobs are scheduled and used by the {@link DagActionReminderScheduler}.
   */
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

  /**
   * Creates a key for the reminder job by concatenating all dagAction fields
   */
  public static String createDagActionReminderKey(DagActionStore.DagAction dagAction) {
    return createDagActionReminderKey(dagAction.getFlowName(), dagAction.getFlowGroup(), dagAction.getFlowExecutionId(),
        dagAction.getJobName(), dagAction.getDagActionType());
  }

  /**
   * Creates a key for the reminder job by concatenating flowName, flowGroup, flowExecutionId, jobName, dagActionType
   * in that order
   */
  public static String createDagActionReminderKey(String flowName, String flowGroup, String flowId, String jobName,
      DagActionStore.DagActionType dagActionType) {
    return String.format("%s.%s.%s.%s.%s", flowGroup, flowName, flowId, jobName, dagActionType);
  }

  /**
   * Creates a jobDetail containing flow and job identifying information in the jobDataMap, uniquely identified
   *  by a key comprised of the dagAction's fields. It also serializes a reference to the {@link DagManagement} object
   *  to be referenced when the trigger fires.
   */
  public static JobDetail createReminderJobDetail(DagManagement dagManagement, DagActionStore.DagAction dagAction) {
    JobDataMap dataMap = new JobDataMap();
    dataMap.put(ReminderJob.DAG_MANAGEMENT_KEY, dagManagement);
    dataMap.put(ConfigurationKeys.FLOW_NAME_KEY, dagAction.getFlowName());
    dataMap.put(ConfigurationKeys.FLOW_GROUP_KEY, dagAction.getFlowGroup());
    dataMap.put(ConfigurationKeys.JOB_NAME_KEY, dagAction.getJobName());
    dataMap.put(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, dagAction.getFlowExecutionId());
    dataMap.put(ReminderJob.FLOW_ACTION_TYPE_KEY, dagAction.getDagActionType());

    return JobBuilder.newJob(ReminderJob.class)
        .withIdentity(createDagActionReminderKey(dagAction), dagAction.getFlowName())
        .usingJobData(dataMap)
        .build();
  }

  /**
   * Creates a Trigger object with the same key as the ReminderJob (since only one trigger is expected to be associated
   * with a job at any given time) that should fire after `reminderDurationMillis` millis.
   */
  public static Trigger createReminderJobTrigger(DagActionStore.DagAction dagAction, long reminderDurationMillis) {
    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity(createDagActionReminderKey(dagAction), dagAction.getFlowName())
        .startAt(new Date(System.currentTimeMillis() + reminderDurationMillis))
        .build();
    return trigger;
  }
}
