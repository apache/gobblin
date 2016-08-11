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
package gobblin.runtime.instance;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.GobblinInstanceDriver;
import gobblin.runtime.api.GobblinInstanceLauncher.ConfigAccessor;
import gobblin.runtime.api.JobCatalog;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionLauncher;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobLifecycleListener;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobSpecMonitorFactory;
import gobblin.runtime.api.JobSpecScheduler;
import gobblin.runtime.api.MutableJobCatalog;
import gobblin.runtime.std.DefaultJobCatalogListenerImpl;
import gobblin.runtime.std.DefaultJobExecutionStateListenerImpl;
import gobblin.runtime.std.JobLifecycleListenersList;

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
  protected final Configurable _sysConfig;
  protected final JobCatalog _jobCatalog;
  protected final JobSpecScheduler _jobScheduler;
  protected final JobExecutionLauncher _jobLauncher;
  protected final ConfigAccessor _instanceCfg;
  protected final JobLifecycleListenersList _dispatcher;
  protected JobSpecListener _jobSpecListener;

  public DefaultGobblinInstanceDriverImpl(Configurable sysConfig, JobCatalog jobCatalog, JobSpecScheduler jobScheduler,
      JobExecutionLauncher jobLauncher, Optional<Logger> log) {
    Preconditions.checkNotNull(jobCatalog);
    Preconditions.checkNotNull(jobScheduler);
    Preconditions.checkNotNull(jobLauncher);
    Preconditions.checkNotNull(sysConfig);

    _jobCatalog = jobCatalog;
    _jobScheduler = jobScheduler;
    _jobLauncher = jobLauncher;
    _sysConfig = sysConfig;
    _instanceCfg = ConfigAccessor.createFromGlobalConfig(_sysConfig.getConfig());
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _dispatcher = new JobLifecycleListenersList(_log);
  }

  /** {@inheritDoc} */
  @Override public JobCatalog getJobCatalog() {
    return _jobCatalog;
  }

  /** {@inheritDoc} */
  @Override public MutableJobCatalog getMutableJobCatalog() {
    return (MutableJobCatalog)_jobCatalog;
  }

  /** {@inheritDoc} */
  @Override public JobSpecScheduler getJobScheduler() {
    return _jobScheduler;
  }

  /** {@inheritDoc} */
  @Override public JobExecutionLauncher getJobLauncher() {
    return _jobLauncher;
  }

  /** {@inheritDoc} */
  @Override public Configurable getSysConfig() {
    return _sysConfig;
  }

  /** {@inheritDoc} */
  @Override public Logger getLog() {
    return _log;
  }

  @Override protected void startUp() throws Exception {
    getLog().info("Default driver: starting ...");
    _jobSpecListener = new JobSpecListener();
    _jobCatalog.addListener(_jobSpecListener);
    getLog().info("Default driver: started.");
  }

  @Override protected void shutDown() throws Exception {
    getLog().info("Default driver: shuttind down ...");
    if (null != _jobSpecListener) {
      _jobCatalog.removeListener(_jobSpecListener);
    }
    getLog().info("Default driver: shut down.");
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
       JobExecutionDriver driver = _jobLauncher.launchJob(_jobSpec);
       driver.registerStateListener(new JobExecutionListener());
       driver.startAsync();
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

    @Override public void onUpdateJob(JobSpec updatedJob) {
      super.onUpdateJob(updatedJob);
      _jobScheduler.scheduleJob(updatedJob, new JobSpecRunnable(updatedJob));
    }

  }

  ConfigAccessor getInstanceCfg() {
    return _instanceCfg;
  }

  @Override
  public void registerJobLifecycleListener(JobLifecycleListener listener) {
    _dispatcher.registerJobLifecycleListener(listener);
  }

  @Override
  public void unregisterJobLifecycleListener(JobLifecycleListener listener) {
    _dispatcher.unregisterJobLifecycleListener(listener);
  }

  @Override
  public List<JobLifecycleListener> getJobLifecycleListeners() {
    return _dispatcher.getJobLifecycleListeners();
  }

}
