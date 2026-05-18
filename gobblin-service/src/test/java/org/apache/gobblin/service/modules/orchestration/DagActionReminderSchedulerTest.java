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
import java.util.List;
import java.util.function.Supplier;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerUtils;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.OperableTrigger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;

import org.apache.gobblin.configuration.ConfigurationKeys;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DagActionReminderSchedulerTest {
  String flowGroup = "fg";
  String flowName = "fn";
  long flowExecutionId = 123L;
  String jobName = "jn";
  long eventTimeMillis = 1234L;
  long eventTimeMillis2 = 5678L;
  String expectedKey =  Joiner.on(".").join(flowGroup, flowName, flowExecutionId, jobName,
      DagActionStore.DagActionType.LAUNCH.name(), eventTimeMillis);
  String expectedKey2 =  Joiner.on(".").join(flowGroup, flowName, flowExecutionId, jobName,
      DagActionStore.DagActionType.LAUNCH.name(), eventTimeMillis2);
  // Deadline reminder keys intentionally omit the lease event time, since the DagActionStore primary key already
  // guarantees uniqueness over (flowGroup, flowName, flowExecutionId, jobName, dagActionType) for ENFORCE_*_DEADLINE.
  String expectedDeadlineKey = Joiner.on(".").join(flowGroup, flowName, flowExecutionId, jobName,
      DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE.name());
  DagActionStore.DagAction launchDagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName,
      DagActionStore.DagActionType.LAUNCH);
  DagActionStore.DagAction enforceFlowFinishDeadlineDagAction = new DagActionStore.DagAction(flowGroup, flowName,
      flowExecutionId, jobName, DagActionStore.DagActionType.ENFORCE_FLOW_FINISH_DEADLINE);
  DagActionStore.LeaseParams launchLeaseParams = new DagActionStore.LeaseParams(launchDagAction, eventTimeMillis);
  DagActionStore.LeaseParams launchLeaseParams2 = new DagActionStore.LeaseParams(launchDagAction, eventTimeMillis2);
  DagActionStore.LeaseParams enforceFlowFinishDeadlineLeaseParams =
      new DagActionStore.LeaseParams(enforceFlowFinishDeadlineDagAction, eventTimeMillis);
  DagActionReminderScheduler dagActionReminderScheduler;
  DagManagement dagManagement = mock(DagManagement.class);
  private static boolean testJobRan = false;

  @BeforeClass
  private void setup() throws Exception {
    doNothing().when(dagManagement).addDagAction(any());
    this.dagActionReminderScheduler = new DagActionReminderScheduler(this.dagManagement);
  }

  @Test
  public void testCreateDagActionReminderKey() {
    // Retry reminders embed the lease event time so multiple events for the same dagAction can coexist.
    Assert.assertEquals(expectedKey, DagActionReminderScheduler.createDagActionReminderKey(launchLeaseParams, false));
    Assert.assertEquals(expectedKey2, DagActionReminderScheduler.createDagActionReminderKey(launchLeaseParams2, false));
  }

  @Test
  public void testCreateDagActionReminderKeyForDeadlineOmitsEventTime() {
    // Deadline reminders are uniquely keyed by the dagAction's primary key alone; event time is stripped so the
    // DELETE-event handler (which has no event time in its payload) can still find and unschedule the reminder.
    Assert.assertEquals(expectedDeadlineKey,
        DagActionReminderScheduler.createDagActionReminderKey(enforceFlowFinishDeadlineLeaseParams, true));
    // The same dagAction with a different event time must produce the same key for deadline reminders.
    DagActionStore.LeaseParams alternateEventTime =
        new DagActionStore.LeaseParams(enforceFlowFinishDeadlineDagAction, eventTimeMillis2);
    Assert.assertEquals(expectedDeadlineKey,
        DagActionReminderScheduler.createDagActionReminderKey(alternateEventTime, true));
  }

  @Test
  public void testCreateReminderJobTrigger() {
    long reminderDuration = 666L;
    Supplier<Long> getCurrentTimeMillis = () -> 12345600000L;
    Trigger reminderTrigger = DagActionReminderScheduler
        .createReminderJobTrigger(launchLeaseParams, reminderDuration, getCurrentTimeMillis, false);
    Assert.assertEquals(reminderTrigger.getKey().toString(), DagActionReminderScheduler.RetryReminderKeyGroup + "." + expectedKey);
    List<Date> fireTimes = TriggerUtils.computeFireTimes((OperableTrigger) reminderTrigger, null, 1);
    Assert.assertEquals(fireTimes.get(0), new Date(reminderDuration + getCurrentTimeMillis.get()));
  }

  @Test
  public void testCreateReminderJobDetail() {
    JobDetail jobDetail = DagActionReminderScheduler.createReminderJobDetail(launchLeaseParams, false);
    Assert.assertEquals(jobDetail.getKey().toString(), DagActionReminderScheduler.RetryReminderKeyGroup + "." + expectedKey);
    JobDataMap dataMap = jobDetail.getJobDataMap();
    Assert.assertEquals(dataMap.get(ConfigurationKeys.FLOW_GROUP_KEY), flowGroup);
    Assert.assertEquals(dataMap.get(ConfigurationKeys.FLOW_NAME_KEY), flowName);
    Assert.assertEquals(dataMap.get(ConfigurationKeys.FLOW_EXECUTION_ID_KEY), flowExecutionId);
    Assert.assertEquals(dataMap.get(ConfigurationKeys.JOB_NAME_KEY), jobName);
    Assert.assertEquals(dataMap.get(DagActionReminderScheduler.ReminderJob.FLOW_ACTION_TYPE_KEY),
        DagActionStore.DagActionType.LAUNCH);
    Assert.assertEquals(dataMap.get(DagActionReminderScheduler.ReminderJob.FLOW_ACTION_EVENT_TIME_KEY), launchLeaseParams.getEventTimeMillis());
  }

  @Test
  public void testCreateReminderJobDetailStashesStoreInsertTimeMillis() {
    long sourceStoreInsertTime = 1700000000000L;
    DagActionStore.LeaseParams paramsWithStoreInsertTime = new DagActionStore.LeaseParams(
        launchDagAction, false, eventTimeMillis, sourceStoreInsertTime);

    JobDetail jobDetail = DagActionReminderScheduler.createReminderJobDetail(paramsWithStoreInsertTime, false);
    JobDataMap dataMap = jobDetail.getJobDataMap();
    Assert.assertEquals(
        dataMap.getLong(DagActionReminderScheduler.ReminderJob.FLOW_ACTION_STORE_INSERT_TIME_MILLIS_KEY),
        sourceStoreInsertTime,
        "Reminder JobDataMap should stash storeInsertTimeMillis so host-failure reattempts preserve it");
  }

  @Test
  public void testReminderJobExecuteCarriesStoreInsertTimeMillis() {
    long sourceStoreInsertTime = 1700000000000L;
    DagActionStore.LeaseParams paramsWithStoreInsertTime = new DagActionStore.LeaseParams(
        launchDagAction, false, eventTimeMillis, sourceStoreInsertTime);
    JobDetail jobDetail = DagActionReminderScheduler.createReminderJobDetail(paramsWithStoreInsertTime, false);
    JobExecutionContext context = mock(JobExecutionContext.class);
    when(context.getMergedJobDataMap()).thenReturn(jobDetail.getJobDataMap());

    DagManagement capturedDagManagement = mock(DagManagement.class);
    ArgumentCaptor<DagActionStore.LeaseParams> captor = ArgumentCaptor.forClass(DagActionStore.LeaseParams.class);
    try {
      doNothing().when(capturedDagManagement).addDagAction(captor.capture());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    new DagActionReminderScheduler.ReminderJob(capturedDagManagement).execute(context);

    DagActionStore.LeaseParams reconstructed = captor.getValue();
    Assert.assertEquals(reconstructed.getStoreInsertTimeMillis(), sourceStoreInsertTime,
        "Reminder fire path should restore storeInsertTimeMillis from JobDataMap");
    Assert.assertTrue(reconstructed.isReminder(), "Reminder-driven LeaseParams must carry isReminder=true");
  }

  @Test
  public void testReminderJobExecuteFallsBackToUnknownWhenKeyAbsent() {
    // Simulates a reminder scheduled by an older code path that did not stash the storeInsertTimeMillis key.
    JobDetail jobDetail = DagActionReminderScheduler.createReminderJobDetail(launchLeaseParams, false);
    JobDataMap dataMap = jobDetail.getJobDataMap();
    dataMap.remove(DagActionReminderScheduler.ReminderJob.FLOW_ACTION_STORE_INSERT_TIME_MILLIS_KEY);
    JobExecutionContext context = mock(JobExecutionContext.class);
    when(context.getMergedJobDataMap()).thenReturn(dataMap);

    DagManagement capturedDagManagement = mock(DagManagement.class);
    ArgumentCaptor<DagActionStore.LeaseParams> captor = ArgumentCaptor.forClass(DagActionStore.LeaseParams.class);
    try {
      doNothing().when(capturedDagManagement).addDagAction(captor.capture());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    new DagActionReminderScheduler.ReminderJob(capturedDagManagement).execute(context);

    Assert.assertEquals(captor.getValue().getStoreInsertTimeMillis(),
        DagActionStore.LeaseParams.UNKNOWN_STORE_INSERT_TIME_MILLIS,
        "Reminders scheduled by older code paths (no key) must fall back to UNKNOWN, not throw");
  }

  /*
  Schedule retry reminders for multiple distinct events of the same dagAction (the realistic multi-KILL/RESUME case
  where lease event time differentiates the requests) and assert no exception is thrown and both can be deleted.
   */
  @Test
  public void testRemindersForMultipleFlowExecutions() throws SchedulerException {
    this.dagActionReminderScheduler.scheduleReminder(launchLeaseParams, 50000, false);
    this.dagActionReminderScheduler.scheduleReminder(launchLeaseParams2, 50000, false);
    this.dagActionReminderScheduler.unscheduleReminderJob(launchLeaseParams, false);
    this.dagActionReminderScheduler.unscheduleReminderJob(launchLeaseParams2, false);
  }

  /*
  Verify deadline reminders are replaced (not double-registered) when the duplicate-insert path causes the same
  dagAction to schedule a second time before the first reminder has been unscheduled. The pre-existing reminder is
  silently removed; no ObjectAlreadyExistsException should propagate.
   */
  @Test
  public void testScheduleDeadlineReminderReplacesExistingEntry() throws SchedulerException {
    JobKey deadlineKey = DagActionReminderScheduler.createJobKey(enforceFlowFinishDeadlineLeaseParams, true);
    this.dagActionReminderScheduler.scheduleReminder(enforceFlowFinishDeadlineLeaseParams, 50000, true);
    Assert.assertTrue(this.dagActionReminderScheduler.quartzScheduler.checkExists(deadlineKey));

    // Re-schedule with a different lease event time - this models the duplicate-insert + out-of-order events case.
    DagActionStore.LeaseParams secondEvent =
        new DagActionStore.LeaseParams(enforceFlowFinishDeadlineDagAction, eventTimeMillis2);
    this.dagActionReminderScheduler.scheduleReminder(secondEvent, 50000, true);
    Assert.assertTrue(this.dagActionReminderScheduler.quartzScheduler.checkExists(deadlineKey));

    this.dagActionReminderScheduler.unscheduleReminderJob(enforceFlowFinishDeadlineDagAction);
    Assert.assertFalse(this.dagActionReminderScheduler.quartzScheduler.checkExists(deadlineKey));
  }

  /*
  Verify the new DagAction-only unschedule overload removes a deadline reminder when called with just the dagAction
  (matching the DELETE-event path in DagManagementDagActionStoreChangeMonitor, which has no lease event time).
   */
  @Test
  public void testUnscheduleDeadlineReminderByDagAction() throws SchedulerException {
    JobKey deadlineKey = DagActionReminderScheduler.createJobKey(enforceFlowFinishDeadlineLeaseParams, true);
    this.dagActionReminderScheduler.scheduleReminder(enforceFlowFinishDeadlineLeaseParams, 50000, true);
    Assert.assertTrue(this.dagActionReminderScheduler.quartzScheduler.checkExists(deadlineKey));

    this.dagActionReminderScheduler.unscheduleReminderJob(enforceFlowFinishDeadlineDagAction);
    Assert.assertFalse(this.dagActionReminderScheduler.quartzScheduler.checkExists(deadlineKey));

    // Idempotent: a second unschedule for an already-fired/removed reminder must not throw.
    this.dagActionReminderScheduler.unscheduleReminderJob(enforceFlowFinishDeadlineDagAction);
  }

  // Test multiple schedulers can co-exist and run their jobs of different types
  @Test
  public void testMultipleSchedules() throws SchedulerException, InterruptedException, IOException {
    JobDetail jobDetail = DagActionReminderScheduler.createReminderJobDetail(launchLeaseParams, false);
    Scheduler scheduler1 = this.dagActionReminderScheduler.quartzScheduler;
    Scheduler scheduler2 = new StdSchedulerFactory().getScheduler();

    Assert.assertNotSame(scheduler1, scheduler2);

    this.dagActionReminderScheduler.scheduleReminder(launchLeaseParams, 100L, false);

    Assert.assertTrue(dagActionReminderScheduler.quartzScheduler.checkExists(jobDetail.getKey()));

    Thread.sleep(200L);

    // verify that the quartz job ran
    Mockito.verify(this.dagManagement, Mockito.times(1)).addDagAction(any());
    // verify that the quartz job cleaned itself without throwing any exception
    Assert.assertFalse(dagActionReminderScheduler.quartzScheduler.checkExists(jobDetail.getKey()));

    scheduler2.start();

    JobDetail job = JobBuilder.newJob(TestJob.class)
        .withIdentity("myJob", "group1")
        .build();

    Trigger trigger = TriggerBuilder.newTrigger()
        .withIdentity("myTrigger", "group1")
        .startAt(new Date(System.currentTimeMillis() + 100L))
        .build();

    scheduler2.scheduleJob(job, trigger);

    Assert.assertTrue(scheduler2.checkExists(job.getKey()));
    Thread.sleep(200L);

    // verify that the quartz job ran
    Assert.assertTrue(DagActionReminderSchedulerTest.testJobRan);
    // verify that the quartz job cleaned itself without throwing any exception
    Assert.assertFalse(scheduler2.checkExists(jobDetail.getKey()));
  }

  public static class TestJob implements Job {
    @Override
    public void execute(JobExecutionContext context) {
      DagActionReminderSchedulerTest.testJobRan = true;
    }
  }
}
