/*
 * Copyright (C) 2014-2015 LinkedIn Corp. All rights reserved.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobExecutionLauncher;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecMonitorFactory;
import gobblin.runtime.api.JobSpecScheduler;
import gobblin.runtime.std.DefaultJobCatalogListenerImpl;
import gobblin.runtime.std.DefaultJobExecutionStateListenerImpl;

/**
 * A default implementation of {@link GobblinInstanceDriver}. It accepts already instantiated
 * {@link JobCatalog}, {@link JobSpecMonitorFactory}, {@link JobSpecScheduler},
 * {@link JobExecutionLauncher}. It is responsibility of the caller to manage those (e.g. start,
 * shutdown, etc.)
 *
 */
public class DefaultGobblinInstanceDriverImpl extends AbstractIdleService
       implements GobblinInstanceDriver {
  protected final Logger _log;
  protected final JobCatalog _jobCatalog;
  protected final JobSpecScheduler _jobScheduler;
  protected final JobExecutionLauncher _jobLauncher;
  protected JobSpecListener _jobSpecListener;

  public DefaultGobblinInstanceDriverImpl(JobCatalog jobCatalog, JobSpecScheduler jobScheduler,
      JobExecutionLauncher jobLauncher, Optional<Logger> log) {
    Preconditions.checkNotNull(jobCatalog);
    Preconditions.checkNotNull(jobScheduler);
    Preconditions.checkNotNull(jobLauncher);

    _jobCatalog = jobCatalog;
    _jobScheduler = jobScheduler;
    _jobLauncher = jobLauncher;
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
  }

  /** {@inheritDoc} */
  @Override
  public JobCatalog getJobCatalog() {
    return _jobCatalog;
  }

  /** {@inheritDoc} */
  @Override
  public JobSpecScheduler getJobScheduler() {
    return _jobScheduler;
  }

  /** {@inheritDoc} */
  @Override
  public JobExecutionLauncher getJobLauncher() {
    return _jobLauncher;
  }

  @Override
  protected void startUp() throws Exception {
    _jobSpecListener = new JobSpecListener();
    _jobCatalog.addListener(_jobSpecListener);
  }

  @Override
  protected void shutDown() throws Exception {
    if (null != _jobSpecListener) {
      _jobCatalog.removeListener(_jobSpecListener);
    }
  }

  class ExecutionStateListener extends DefaultJobExecutionStateListenerImpl {

    public ExecutionStateListener() {
      super(DefaultGobblinInstanceDriverImpl.this._log);
    }

    @Override
    public void onStatusChange(JobExecutionState state, RunningState previousStatus, RunningState newStatus) {
      // TODO Auto-generated method stub

    }

    @Override
    public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
      // TODO Auto-generated method stub

    }

    @Override
    public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
      // TODO Auto-generated method stub

    }

  }

  /** Keeps track of a job execution */
  class JobExecutionListener extends DefaultJobExecutionStateListenerImpl {

    public JobExecutionListener() {
      super(LoggerFactory.getLogger(DefaultGobblinInstanceDriverImpl.this._log.getName() +
                                  "_jobExecutionListener"));
    }

    @Override public String toString() {
      return _log.get().getName();
    }

  }

  /** The runnable invoked by the Job scheduler */
  class JobSpecRunnable implements Runnable {
    private final JobSpec _jobSpec;

    public JobSpecRunnable(JobSpec jobSpec) {
      _jobSpec = jobSpec;
    }

    @Override
    public void run() {
       _jobLauncher.launchJob(_jobSpec, new JobExecutionListener());
    }
  }

  /** Listens to changes in the Job catalog and schedules/un-schedules jobs. */
  class JobSpecListener extends DefaultJobCatalogListenerImpl {

    public JobSpecListener() {
      super(LoggerFactory.getLogger(DefaultGobblinInstanceDriverImpl.this._log.getName() +
                                  "_jobSpecListener"));
    }

    @Override public String toString() {
      return _log.get().getName();
    }

    @Override public void onAddJob(JobSpec addedJob) {
      super.onAddJob(addedJob);
      _jobScheduler.scheduleJob(addedJob, new JobSpecRunnable(addedJob));
    }

    @Override public void onDeleteJob(JobSpec deletedJob) {
      super.onDeleteJob(deletedJob);
      _jobScheduler.unscheduleJob(deletedJob.getUri());
    }

    @Override public void onUpdateJob(JobSpec originalJob, JobSpec updatedJob) {
      super.onUpdateJob(originalJob, updatedJob);
      _jobScheduler.scheduleJob(updatedJob, new JobSpecRunnable(updatedJob));
    }

  }

}
