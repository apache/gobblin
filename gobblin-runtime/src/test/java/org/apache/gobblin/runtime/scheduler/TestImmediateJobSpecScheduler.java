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
package gobblin.runtime.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecSchedule;
import gobblin.runtime.api.JobSpecSchedulerListener;

/**
 * Unit tests for {@link ImmediateJobSpecScheduler}
 *
 */
public class TestImmediateJobSpecScheduler {

  @Test
  public void testSchedule() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getName() + ".testSimpleFlow");
    final Optional<Logger> logOpt = Optional.of(log);
    ImmediateJobSpecScheduler scheduler = new ImmediateJobSpecScheduler(logOpt);

    JobSpecSchedulerListener listener = mock(JobSpecSchedulerListener.class);
    scheduler.registerWeakJobSpecSchedulerListener(listener);

    final CountDownLatch expectedCallCount = new CountDownLatch(4);
    Runnable r = new Runnable() {
      @Override public void run() {
        expectedCallCount.countDown();
      }
    };

    JobSpec js1 = JobSpec.builder("test.job1").build();
    JobSpec js2 = JobSpec.builder("test.job2").build();
    JobSpec js3 = JobSpec.builder("test.job3").build();
    JobSpec js1_2 = JobSpec.builder("test.job1").withVersion("2").build();

    JobSpecSchedule jss1 = scheduler.scheduleJob(js1, r);
    Assert.assertEquals(scheduler.getSchedules().size(), 1);
    Assert.assertEquals(jss1.getJobSpec(), js1);
    Assert.assertTrue(jss1.getNextRunTimeMillis().isPresent());
    Assert.assertTrue(jss1.getNextRunTimeMillis().get().longValue() <= System.currentTimeMillis());;

    JobSpecSchedule jss2 = scheduler.scheduleJob(js2, r);
    Assert.assertEquals(scheduler.getSchedules().size(), 2);
    Assert.assertEquals(jss2.getJobSpec(), js2);
    Assert.assertTrue(jss2.getNextRunTimeMillis().isPresent());
    Assert.assertTrue(jss2.getNextRunTimeMillis().get().longValue() <= System.currentTimeMillis());;

    JobSpecSchedule jss3 = scheduler.scheduleJob(js3, r);
    Assert.assertEquals(scheduler.getSchedules().size(), 3);
    Assert.assertEquals(jss3.getJobSpec(), js3);


    JobSpecSchedule jss1_2 = scheduler.scheduleJob(js1_2, r);
    Assert.assertEquals(scheduler.getSchedules().size(), 3);
    Assert.assertEquals(jss1_2.getJobSpec(), js1_2);

    Assert.assertTrue(expectedCallCount.await(100, TimeUnit.MILLISECONDS));

    scheduler.unscheduleJob(js1.getUri());
    Assert.assertEquals(scheduler.getSchedules().size(), 2);
    scheduler.unscheduleJob(js1.getUri());
    Assert.assertEquals(scheduler.getSchedules().size(), 2);

    verify(listener).onJobScheduled(Mockito.eq(jss1));
    verify(listener).onJobTriggered(Mockito.eq(js1));
    verify(listener).onJobScheduled(Mockito.eq(jss2));
    verify(listener).onJobTriggered(Mockito.eq(js2));
    verify(listener).onJobScheduled(Mockito.eq(jss3));
    verify(listener).onJobTriggered(Mockito.eq(js3));
    verify(listener).onJobUnscheduled(Mockito.eq(jss1));
    verify(listener).onJobScheduled(Mockito.eq(jss1_2));
    verify(listener).onJobTriggered(Mockito.eq(js1_2));
    verify(listener, Mockito.times(1)).onJobUnscheduled(Mockito.eq(jss1_2));
  }
}
