/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.runtime.scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobSpec;

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

    final CountDownLatch expectedCallCount = new CountDownLatch(3);

    Runnable r = new Runnable() {
      @Override public void run() {
        expectedCallCount.countDown();
      }
    };

    JobSpec js1 = JobSpec.builder("test.job1").build();
    JobSpec js2 = JobSpec.builder("test.job2").build();
    JobSpec js3 = JobSpec.builder("test.job3").build();

    scheduler.scheduleJob(js1, r);
    Assert.assertEquals(scheduler.getSchedules().size(), 0);
    scheduler.scheduleJob(js2, r);
    Assert.assertEquals(scheduler.getSchedules().size(), 0);
    scheduler.scheduleJob(js3, r);
    Assert.assertEquals(scheduler.getSchedules().size(), 0);

    Assert.assertTrue(expectedCallCount.await(100, TimeUnit.MILLISECONDS));
  }
}
