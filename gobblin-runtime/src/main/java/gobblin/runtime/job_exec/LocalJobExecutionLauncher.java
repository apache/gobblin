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
package gobblin.runtime.job_exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionLauncher;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.std.JobExecutionUpdatable;

/**
 * Runs the Gobblin jobs locally in a thread pool.
 */
public class LocalJobExecutionLauncher implements JobExecutionLauncher {
  private final GobblinInstanceDriver _instanceDriver;
  private final Logger _log;

  public LocalJobExecutionLauncher(GobblinInstanceDriver instanceDriver,
                                   Optional<Logger> log) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _instanceDriver = instanceDriver;
  }

  /** {@inheritDoc} */
  @Override
  public JobExecutionDriver launchJob(JobSpec jobSpec) {
	// TODO Add impl
    return null;
  }

  private static class JobLauncherRunnable implements Runnable {

    @Override
    public void run() {
      // TODO Auto-generated method stub

    }

  }
}
