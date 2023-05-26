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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import javax.inject.Inject;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.SchedulerLeaseDeterminationStore;
import org.apache.gobblin.scheduler.JobScheduler;
import org.apache.gobblin.scheduler.SchedulerService;
import org.apache.gobblin.util.ConfigUtils;


public class SchedulerLeaseAlgoHandler {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerLeaseAlgoHandler.class);
  private final long linger;
  private final int staggerUpperBoundSec;
  private static Random random = new Random();
  protected SchedulerLeaseDeterminationStore leaseDeterminationStore;
  protected JobScheduler jobScheduler;
  protected SchedulerService schedulerService;
  @Inject
  public SchedulerLeaseAlgoHandler(Config config, SchedulerLeaseDeterminationStore leaseDeterminationStore,
      JobScheduler jobScheduler, SchedulerService schedulerService)
      throws IOException {
    this.linger = ConfigUtils.getLong(config, ConfigurationKeys.SCHEDULER_TRIGGER_EVENT_EPSILON_MILLIS_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_TRIGGER_EVENT_EPSILON_MILLIS);
    this.staggerUpperBoundSec = ConfigUtils.getInt(config,
        ConfigurationKeys.SCHEDULER_STAGGERING_UPPER_BOUND_SEC_KEY,
        ConfigurationKeys.DEFAULT_SCHEDULER_STAGGERING_UPPER_BOUND_SEC);
    this.leaseDeterminationStore = leaseDeterminationStore;
    this.jobScheduler = jobScheduler;
    this.schedulerService = schedulerService;
  }
  private SchedulerLeaseDeterminationStore schedulerLeaseDeterminationStore;

  /**
   * This method is used in the multi-active scheduler case for one or more hosts to respond to a flow's trigger event
   * by attempting a lease for the flow event.
   * @param jobProps
   * @param flowGroup
   * @param flowName
   * @param flowExecutionId
   * @param flowActionType
   * @param triggerTimeMillis
   * @return true if this host obtained the lease for this flow's trigger event, false otherwise.
   * @throws IOException
   */
  public boolean handleNewTriggerEvent(Properties jobProps, String flowGroup, String flowName, String flowExecutionId,
      SchedulerLeaseDeterminationStore.FlowActionType flowActionType, long triggerTimeMillis)
      throws IOException {
    SchedulerLeaseDeterminationStore.LeaseAttemptStatus leaseAttemptStatus =
        schedulerLeaseDeterminationStore.attemptInsertAndGetPursuantTimestamp(flowGroup, flowName, flowExecutionId,
            flowActionType, triggerTimeMillis);
    // TODO: add a log event or metric for each of these cases
    switch (leaseAttemptStatus) {
      case LEASE_OBTAINED:
        return true;
      case PREVIOUS_LEASE_EXPIRED:
        // recursively try obtaining lease again immediately, stops when reaches one of the other cases
        return handleNewTriggerEvent(jobProps, flowGroup, flowName, flowExecutionId, flowActionType, triggerTimeMillis);
      case PREVIOUS_LEASE_VALID:
        scheduleReminderForTriggerEvent(jobProps, flowGroup, flowName, flowExecutionId, flowActionType, triggerTimeMillis);
    }
    return false;
  }

  /**
   * This method is used by {@link SchedulerLeaseAlgoHandler.handleNewTriggerEvent} to schedule a reminder for itself to
   * check on the other participant's progress during pursuing orchestration after the time the lease should expire.
   * If the previous participant was successful, then no further action is taken otherwise we re-attempt pursuing
   * orchestration ourselves.
   * @param flowGroup
   * @param flowName
   * @param flowExecutionId
   * @param flowActionType
   * @param triggerTimeMillis
   */
  protected void scheduleReminderForTriggerEvent(Properties jobProps, String flowGroup, String flowName, String flowExecutionId,
      SchedulerLeaseDeterminationStore.FlowActionType flowActionType, long triggerTimeMillis) {
    // Check-in `linger` time after the current timestamp which is "close-enough" to the time the pursuant attempted
    // the flow action. We also add a small randomization to avoid 'thundering herd' issue
    String cronExpression = createCronFromDelayPeriod(linger + random.nextInt(staggerUpperBoundSec));
    jobProps.setProperty(ConfigurationKeys.JOB_SCHEDULE_KEY, cronExpression);
    // This timestamp is what will be used to identify the particular flow trigger event it's associated with
    jobProps.setProperty(ConfigurationKeys.SCHEDULER_ORIGINAL_TRIGGER_TIMESTAMP_MILLIS_KEY, String.valueOf(triggerTimeMillis));
    JobKey key = new JobKey(flowName, flowGroup);
    Trigger trigger = this.jobScheduler.getTrigger(key, jobProps);
    try {
      LOG.info("Attempting to add job reminder to Scheduler Service where job is %s trigger event %s and reminder is at "
          + "%s.", key, triggerTimeMillis, trigger.getNextFireTime());
      this.schedulerService.getScheduler().scheduleJob(trigger);
    } catch (SchedulerException e) {
      LOG.warn("Failed to add job reminder due to SchedulerException for job %s trigger event %s ", key, triggerTimeMillis, e);
    }
    LOG.info(String.format("Scheduled reminder for job %s trigger event %s. Next run: %s.", key, triggerTimeMillis, trigger.getNextFireTime()));
  }

  /**
   * These methods should only be called from the Orchestrator or JobScheduler classes as it directly adds jobs to the
   * Quartz scheduler
   * @param delayPeriodSeconds
   * @return
   */
  protected static String createCronFromDelayPeriod(long delayPeriodSeconds) {
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime delaySecondsLater = now.plus(delayPeriodSeconds, ChronoUnit.SECONDS);
    // TODO: investigate potentially better way of generating cron expression that does not make it US dependent
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ss mm HH dd MM ? yyyy", Locale.US);
    return delaySecondsLater.format(formatter);
  }

}
