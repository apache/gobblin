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
package org.apache.gobblin.runtime.scheduler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecSchedule;
import org.apache.gobblin.runtime.scheduler.QuartzJobSpecScheduler.QuartzJobSchedule;

/**
 * Unit tests for {@link QuartzJobSpecScheduler}.
 */
public class TestQuartzJobSpecScheduler {

  @Test public void testSchedule() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getName() + ".testSchedule");
    Config quartzCfg = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put("org.quartz.scheduler.instanceName", "TestQuartzJobSpecScheduler.testSchedule")
        .put("org.quartz.threadPool.threadCount", "10")
        .put("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore")
        .build());

    QuartzJobSpecScheduler scheduler = new QuartzJobSpecScheduler(log, quartzCfg);
    scheduler.startAsync();
    scheduler.awaitRunning(10, TimeUnit.SECONDS);
    Assert.assertTrue(scheduler._scheduler.getScheduler().isStarted());

    final ArrayBlockingQueue<JobSpec> expectedCalls = new ArrayBlockingQueue<>(100);
    try {
      Config jobCfg1 = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(ConfigurationKeys.JOB_SCHEDULE_KEY, "0/5 * * * * ?")
          .build());
      Config jobCfg2 = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(ConfigurationKeys.JOB_SCHEDULE_KEY, "3/5 * * * * ?")
          .build());

      final JobSpec js1 = JobSpec.builder("test.job1").withConfig(jobCfg1).build();
      final JobSpec js2 = JobSpec.builder("test.job2").withConfig(jobCfg2).build();
      final JobSpec js1_2 = JobSpec.builder("test.job1").withConfig(jobCfg1).withVersion("2").build();

      JobSpecSchedule jss1 = scheduler.scheduleJob(js1, new Runnable() {
        @Override public void run() {
          expectedCalls.offer(js1);
        }
      });

      Assert.assertEquals(scheduler.getSchedules().size(), 1);
      Assert.assertEquals(jss1.getJobSpec(), js1);
      Assert.assertTrue(jss1 instanceof QuartzJobSchedule);
      QuartzJobSchedule qjss1 = (QuartzJobSchedule)jss1;
      Assert.assertNotNull(scheduler._scheduler.getScheduler().getJobDetail(qjss1.getQuartzTrigger().getJobKey()));
      Assert.assertNotNull(scheduler._scheduler.getScheduler().getTrigger(qjss1.getQuartzTrigger().getKey()));
      Assert.assertTrue(qjss1.getQuartzTrigger().mayFireAgain());

      // Wait for the next run
      JobSpec expJs1 = expectedCalls.poll(6000, TimeUnit.MILLISECONDS);
      Assert.assertEquals(expJs1, js1);

      // Wait for the next run
      expJs1 = expectedCalls.poll(6000, TimeUnit.MILLISECONDS);
      Assert.assertEquals(expJs1, js1);

      // Schedule another job
      JobSpecSchedule jss2 = scheduler.scheduleJob(js2, new Runnable() {
        @Override public void run() {
          expectedCalls.offer(js2);
        }
      });
      Assert.assertEquals(scheduler.getSchedules().size(), 2);
      Assert.assertEquals(jss2.getJobSpec(), js2);

      // Wait for the next run -- we should get js2
      JobSpec expJs2 = expectedCalls.poll(6000, TimeUnit.MILLISECONDS);
      Assert.assertEquals(expJs2, js2);

      // Wait for the next run -- we should get js1
      expJs1 = expectedCalls.poll(6000, TimeUnit.MILLISECONDS);
      Assert.assertEquals(expJs1, js1);

      // Wait for the next run -- we should get js2
      expJs2 = expectedCalls.poll(6000, TimeUnit.MILLISECONDS);
      log.info("Found call: " + expJs2);
      Assert.assertEquals(expJs2, js2);

      // Update the first job
      QuartzJobSchedule qjss1_2 = (QuartzJobSchedule)scheduler.scheduleJob(js1_2, new Runnable() {
        @Override public void run() {
          expectedCalls.offer(js1_2);
        }
      });
      Assert.assertEquals(scheduler.getSchedules().size(), 2);


      // Wait for 5 seconds -- we should see at least 2 runs of js1_2 and js2
      Thread.sleep(15000);
      int js1_2_cnt = 0;
      int js2_cnt = 0;
      for (JobSpec nextJs: expectedCalls){
        log.info("Found call: " + nextJs);
        if (js1_2.equals(nextJs)) {
          ++js1_2_cnt;
        }
        else if (js2.equals(nextJs))  {
          ++js2_cnt;
        }
        else {
          Assert.fail("Unexpected job spec: " + nextJs);
        }
      }
      Assert.assertTrue(js1_2_cnt >= 2, "js1_2_cnt=" + js1_2_cnt);
      Assert.assertTrue(js2_cnt >= 2, "js2_cnt=" + js2_cnt);

      scheduler.unscheduleJob(js1_2.getUri());
      Assert.assertEquals(scheduler.getSchedules().size(), 1);
      Assert.assertFalse(scheduler._scheduler.getScheduler().checkExists(qjss1_2.getQuartzTrigger().getJobKey()));

      // Flush calls
      Thread.sleep(1000);
      expectedCalls.clear();

      // All subsequent calls should be for js2
      for (int i = 0; i < 2; ++i){
        JobSpec nextJs = expectedCalls.poll(12000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(nextJs, js2);
      }
    }
    finally {
      scheduler.stopAsync();
      scheduler.awaitTerminated(10, TimeUnit.SECONDS);
    }

    // make sure there are no more calls
    // we may have to drain at most one call due to race conditions
    if (null != expectedCalls.poll(2100, TimeUnit.MILLISECONDS)) {
      Assert.assertNull(expectedCalls.poll(3000, TimeUnit.MILLISECONDS));
    }
  }

}
