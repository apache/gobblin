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

package gobblin.scheduler;

import java.util.Properties;

import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;

import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.ConfigurationKeys;

import lombok.Getter;


/**
 * A {@link com.google.common.util.concurrent.Service} wrapping a Quartz {@link Scheduler} allowing correct shutdown
 * of the scheduler when {@link JobScheduler} fails to initialize.
 */
public class SchedulerService extends AbstractIdleService {

  @Getter
  private Scheduler scheduler;
  private final boolean waitForJobCompletion;

  public SchedulerService(Properties props) {
    this.waitForJobCompletion = Boolean.parseBoolean(
        props.getProperty(ConfigurationKeys.SCHEDULER_WAIT_FOR_JOB_COMPLETION_KEY,
            ConfigurationKeys.DEFAULT_SCHEDULER_WAIT_FOR_JOB_COMPLETION));
  }

  @Override
  protected void startUp()
      throws Exception {
    this.scheduler = new StdSchedulerFactory().getScheduler();
    this.scheduler.start();
  }

  @Override
  protected void shutDown()
      throws Exception {
    this.scheduler.shutdown(this.waitForJobCompletion);
  }
}
