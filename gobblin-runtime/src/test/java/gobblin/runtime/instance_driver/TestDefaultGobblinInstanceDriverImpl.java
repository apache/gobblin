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
package gobblin.runtime.instance_driver;

import java.util.concurrent.TimeUnit;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.runtime.api.JobExecutionLauncher;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecScheduler;
import gobblin.runtime.job_catalog.InMemoryJobCatalog;
import gobblin.runtime.std.DefaultJobSpecScheduleImpl;

/**
 * Unit tests for {@link DefaultGobblinInstanceDriverImpl}
 */
public class TestDefaultGobblinInstanceDriverImpl {

  @Test
  public void testScheduling() throws Exception {
    final Logger log = LoggerFactory.getLogger(getClass().getName() + ".testBasicFlow");
    final Optional<Logger> loggerOpt = Optional.of(log);
    InMemoryJobCatalog jobCatalog = new InMemoryJobCatalog(loggerOpt);

    JobSpecScheduler scheduler = Mockito.mock(JobSpecScheduler.class);
    JobExecutionLauncher jobLauncher = Mockito.mock(JobExecutionLauncher.class);

    DefaultGobblinInstanceDriverImpl driver =
        new DefaultGobblinInstanceDriverImpl(jobCatalog, scheduler, jobLauncher, loggerOpt);

    JobSpec js1_1 = JobSpec.builder("test.job1").withVersion("1").build();
    JobSpec js1_2 = JobSpec.builder("test.job1").withVersion("2").build();
    JobSpec js2 = JobSpec.builder("test.job2").withVersion("1").build();

    jobCatalog.put(js1_1);

    driver.startAsync().awaitRunning(100, TimeUnit.MILLISECONDS);
    jobCatalog.put(js2);
    jobCatalog.put(js1_2);
    jobCatalog.remove(js2.getUri());

    Mockito.when(
        scheduler.scheduleJob(Mockito.eq(js1_1),
                              Mockito.any(DefaultGobblinInstanceDriverImpl.JobSpecRunnable.class)))
           .thenReturn(DefaultJobSpecScheduleImpl.createNoSchedule(js1_1, null));
    Mockito.when(
        scheduler.scheduleJob(Mockito.eq(js2),
                              Mockito.any(DefaultGobblinInstanceDriverImpl.JobSpecRunnable.class)))
           .thenReturn(DefaultJobSpecScheduleImpl.createNoSchedule(js2, null));
    Mockito.when(
        scheduler.scheduleJob(Mockito.eq(js1_2),
                              Mockito.any(DefaultGobblinInstanceDriverImpl.JobSpecRunnable.class)))
           .thenReturn(DefaultJobSpecScheduleImpl.createNoSchedule(js1_2, null));

    Mockito.verify(scheduler).scheduleJob(Mockito.eq(js1_1),
        Mockito.any(DefaultGobblinInstanceDriverImpl.JobSpecRunnable.class));
    Mockito.verify(scheduler).scheduleJob(Mockito.eq(js2),
        Mockito.any(DefaultGobblinInstanceDriverImpl.JobSpecRunnable.class));
    Mockito.verify(scheduler).scheduleJob(Mockito.eq(js1_2),
        Mockito.any(DefaultGobblinInstanceDriverImpl.JobSpecRunnable.class));
    Mockito.verify(scheduler).unscheduleJob(Mockito.eq(js2.getUri()));
  }

}
