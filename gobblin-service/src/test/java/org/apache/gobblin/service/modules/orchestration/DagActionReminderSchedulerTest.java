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

import org.mockito.Mockito;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
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
  DagActionStore.DagAction launchDagAction = new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, jobName,
      DagActionStore.DagActionType.LAUNCH);
  DagActionStore.LeaseParams launchLeaseParams = new DagActionStore.LeaseParams(launchDagAction, eventTimeMillis);
  DagActionStore.LeaseParams launchLeaseParams2 = new DagActionStore.LeaseParams(launchDagAction, eventTimeMillis2);
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
    Assert.assertEquals(expectedKey, DagActionReminderScheduler.createDagActionReminderKey(launchLeaseParams));
    Assert.assertEquals(expectedKey2, DagActionReminderScheduler.createDagActionReminderKey(launchLeaseParams2));
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

  /*
  Add deadline reminders for multiple launches of the same flow and assert no exception is thrown and they can be
  deleted as well.
   */
  @Test
  public void testRemindersForMultipleFlowExecutions() throws SchedulerException {
    this.dagActionReminderScheduler.scheduleReminder(launchLeaseParams, 50000, true);
    this.dagActionReminderScheduler.scheduleReminder(launchLeaseParams2, 50000, true);
    this.dagActionReminderScheduler.unscheduleReminderJob(launchLeaseParams, true);
    this.dagActionReminderScheduler.unscheduleReminderJob(launchLeaseParams2, true);
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
