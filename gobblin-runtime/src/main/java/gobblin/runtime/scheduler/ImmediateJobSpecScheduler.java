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

import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecSchedule;
import gobblin.runtime.api.JobSpecScheduler;
import gobblin.runtime.std.DefaultJobSpecScheduleImpl;
import gobblin.util.LoggingUncaughtExceptionHandler;

/**
 * A simple implementation of a {@link JobSpecScheduler} which schedules the job immediately.
 * Note that job runnable is scheduled immediately but this happens in a separate thread so it is
 * asynchronous.
 */
public class ImmediateJobSpecScheduler extends AbstractJobSpecScheduler {
  private final ThreadFactory _jobRunnablesThreadFactory;

  public ImmediateJobSpecScheduler(Optional<Logger> log) {
    super(log);
    _jobRunnablesThreadFactory = (new ThreadFactoryBuilder())
        .setDaemon(false)
        .setNameFormat(getLog().getName() + "-thread-%d")
        .setUncaughtExceptionHandler(new LoggingUncaughtExceptionHandler(Optional.of(getLog())))
        .build();
  }

  @Override
  protected JobSpecSchedule doScheduleJob(JobSpec jobSpec, Runnable jobRunnable) {
    Thread runThread = _jobRunnablesThreadFactory.newThread(jobRunnable);
    getLog().info("Starting JobSpec " + jobSpec + " in thread " + runThread.getName());
    JobSpecSchedule schedule =
        DefaultJobSpecScheduleImpl.createImmediateSchedule(jobSpec, jobRunnable);
    runThread.start();
    return schedule;
  }

  @Override
  protected void doUnschedule(JobSpecSchedule existingSchedule) {
    // nothing to do
  }

}
