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
package org.apache.gobblin.runtime.instance;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;

import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.runtime.JobState.RunningState;
import org.apache.gobblin.runtime.api.Configurable;
import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.GobblinInstanceLauncher.ConfigAccessor;
import org.apache.gobblin.runtime.api.JobCatalog;
import org.apache.gobblin.runtime.api.JobExecutionDriver;
import org.apache.gobblin.runtime.api.JobExecutionLauncher;
import org.apache.gobblin.runtime.api.JobExecutionState;
import org.apache.gobblin.runtime.api.JobLifecycleListener;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.JobSpecMonitorFactory;
import org.apache.gobblin.runtime.api.JobSpecScheduler;
import org.apache.gobblin.runtime.api.MutableJobCatalog;
import org.apache.gobblin.runtime.job_spec.ResolvedJobSpec;
import org.apache.gobblin.runtime.std.DefaultJobCatalogListenerImpl;
import org.apache.gobblin.runtime.std.DefaultJobExecutionStateListenerImpl;
import org.apache.gobblin.runtime.std.JobLifecycleListenersList;
import org.apache.gobblin.util.ExecutorsUtils;


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
  protected final String _instanceName;
  protected final Configurable _sysConfig;
  protected final JobCatalog _jobCatalog;
  protected final JobSpecScheduler _jobScheduler;
  protected final JobExecutionLauncher _jobLauncher;
  protected final ConfigAccessor _instanceCfg;
  protected final JobLifecycleListenersList _callbacksDispatcher;
  private final boolean _instrumentationEnabled;
  protected final MetricContext _metricCtx;
  protected JobSpecListener _jobSpecListener;
  private final StandardMetrics _metrics;
  private final SharedResourcesBroker<GobblinScopeTypes> _instanceBroker;

  public DefaultGobblinInstanceDriverImpl(String instanceName,
      Configurable sysConfig, JobCatalog jobCatalog,
      JobSpecScheduler jobScheduler,
      JobExecutionLauncher jobLauncher,
      Optional<MetricContext> baseMetricContext,
      Optional<Logger> log,
      SharedResourcesBroker<GobblinScopeTypes> instanceBroker) {
    Preconditions.checkNotNull(jobCatalog);
    Preconditions.checkNotNull(jobScheduler);
    Preconditions.checkNotNull(jobLauncher);
    Preconditions.checkNotNull(sysConfig);

    _instanceName = instanceName;
    _log = log.or(LoggerFactory.getLogger(getClass()));
    _metricCtx = baseMetricContext.or(constructMetricContext(sysConfig, _log));
    _instrumentationEnabled = null != _metricCtx && GobblinMetrics.isEnabled(sysConfig.getConfig());
    _jobCatalog = jobCatalog;
    _jobScheduler = jobScheduler;
    _jobLauncher = jobLauncher;
    _sysConfig = sysConfig;
    _instanceCfg = ConfigAccessor.createFromGlobalConfig(_sysConfig.getConfig());
    _callbacksDispatcher = new JobLifecycleListenersList(_jobCatalog, _jobScheduler, _log);
    _instanceBroker = instanceBroker;

    _metrics = new StandardMetrics(this);
  }

  private MetricContext constructMetricContext(Configurable sysConfig, Logger log) {
    org.apache.gobblin.configuration.State tmpState = new org.apache.gobblin.configuration.State(sysConfig.getConfigAsProperties());
    return GobblinMetrics.isEnabled(sysConfig.getConfig()) ?
          Instrumented.getMetricContext(tmpState, getClass())
          : null;
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
  @Override public SharedResourcesBroker<GobblinScopeTypes> getInstanceBroker() {
    return _instanceBroker;
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
    _callbacksDispatcher.close();
    getLog().info("Default driver: shut down.");
  }

  /** Keeps track of a job execution */
  class JobStateTracker extends DefaultJobExecutionStateListenerImpl {

    public JobStateTracker() {
      super(LoggerFactory.getLogger(DefaultGobblinInstanceDriverImpl.this._log.getName() +
                                  "_jobExecutionListener"));
    }

    @Override public String toString() {
      return _log.get().getName();
    }

    @Override public void onStatusChange(JobExecutionState state, RunningState previousStatus, RunningState newStatus) {
      super.onStatusChange(state, previousStatus, newStatus);
      _callbacksDispatcher.onStatusChange(state, previousStatus, newStatus);
    }

    @Override
    public void onStageTransition(JobExecutionState state, String previousStage, String newStage) {
      super.onStageTransition(state, previousStage, newStage);
      _callbacksDispatcher.onStageTransition(state, previousStage, newStage);
    }

    @Override
    public void onMetadataChange(JobExecutionState state, String key, Object oldValue, Object newValue) {
      super.onMetadataChange(state, key, oldValue, newValue);
      _callbacksDispatcher.onMetadataChange(state, key, oldValue, newValue);
    }

  }

  /** The runnable invoked by the Job scheduler */
  class JobSpecRunnable implements Runnable {
    private final JobSpec _jobSpec;
    private final GobblinInstanceDriver _instanceDriver;

    public JobSpecRunnable(JobSpec jobSpec, GobblinInstanceDriver instanceDriver) {
      _jobSpec = jobSpec;
      _instanceDriver = instanceDriver;
    }

    @Override
    public void run() {
      try {
         JobExecutionDriver driver = _jobLauncher.launchJob(new ResolvedJobSpec(_jobSpec, _instanceDriver));
         _callbacksDispatcher.onJobLaunch(driver);
         driver.registerStateListener(new JobStateTracker());
        ExecutorsUtils.newThreadFactory(Optional.of(_log), Optional.of("gobblin-instance-driver")).newThread(driver).start();
      }
      catch (Throwable t) {
        _log.error("Job launch failed: " + t, t);
      }
    }
  }

  /** Listens to changes in the Job catalog and schedules/un-schedules jobs. */
  protected class JobSpecListener extends DefaultJobCatalogListenerImpl {
    public JobSpecListener() {
      super(LoggerFactory.getLogger(DefaultGobblinInstanceDriverImpl.this._log.getName() +
                                  "_jobSpecListener"));
    }

    @Override public String toString() {
      return _log.get().getName();
    }

    @Override public void onAddJob(JobSpec addedJob) {
      super.onAddJob(addedJob);
      _jobScheduler.scheduleJob(addedJob, createJobSpecRunnable(addedJob));
    }

    @Override public void onDeleteJob(URI deletedJobURI, String deletedJobVersion) {
      super.onDeleteJob(deletedJobURI, deletedJobVersion);
      _jobScheduler.unscheduleJob(deletedJobURI);
    }

    @Override public void onUpdateJob(JobSpec updatedJob) {
      super.onUpdateJob(updatedJob);
      _jobScheduler.scheduleJob(updatedJob, createJobSpecRunnable(updatedJob));
    }
  }

  @VisibleForTesting JobSpecRunnable createJobSpecRunnable(JobSpec addedJob) {
    return new JobSpecRunnable(addedJob, this);
  }

  ConfigAccessor getInstanceCfg() {
    return _instanceCfg;
  }

  @Override
  public void registerJobLifecycleListener(JobLifecycleListener listener) {
    _callbacksDispatcher.registerJobLifecycleListener(listener);
  }

  @Override
  public void unregisterJobLifecycleListener(JobLifecycleListener listener) {
    _callbacksDispatcher.unregisterJobLifecycleListener(listener);
  }

  @Override
  public List<JobLifecycleListener> getJobLifecycleListeners() {
    return _callbacksDispatcher.getJobLifecycleListeners();
  }

  @Override
  public void registerWeakJobLifecycleListener(JobLifecycleListener listener) {
    _callbacksDispatcher.registerWeakJobLifecycleListener(listener);
  }

  @Override public MetricContext getMetricContext() {
    return _metricCtx;
  }

  @Override public boolean isInstrumentationEnabled() {
    return _instrumentationEnabled;
  }

  @Override public List<Tag<?>> generateTags(org.apache.gobblin.configuration.State state) {
    return Collections.emptyList();
  }

  @Override public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Override public StandardMetrics getMetrics() {
    return _metrics;
  }

  @Override
  public String getInstanceName() {
    return _instanceName;
  }

}
