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

package gobblin.runtime.job_exec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ExecutionList;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.SimpleScope;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.SharedResourcesBrokerImpl;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.configuration.ConfigurationKeys;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.Tag;
import gobblin.runtime.JobContext;
import gobblin.runtime.JobException;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.JobLauncherFactory.JobLauncherType;
import gobblin.runtime.JobState.RunningState;
import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.GobblinInstanceEnvironment;
import gobblin.runtime.api.JobExecution;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionLauncher;
import gobblin.runtime.api.JobExecutionResult;
import gobblin.runtime.api.JobExecutionState;
import gobblin.runtime.api.JobExecutionStateListener;
import gobblin.runtime.api.JobExecutionStatus;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.api.JobTemplate;
import gobblin.runtime.api.SpecNotFoundException;
import gobblin.runtime.instance.StandardGobblinInstanceLauncher;
import gobblin.runtime.job_spec.ResolvedJobSpec;
import gobblin.runtime.listeners.AbstractJobListener;
import gobblin.runtime.std.DefaultConfigurableImpl;
import gobblin.runtime.std.JobExecutionStateListeners;
import gobblin.runtime.std.JobExecutionUpdatable;
import gobblin.util.ExecutorsUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;


/**
 * An implementation of JobExecutionDriver which acts as an adapter to the legacy
 * {@link JobLauncher} API.
 */
public class JobLauncherExecutionDriver extends FutureTask<JobExecutionResult> implements JobExecutionDriver {
  private final Logger _log;
  private final JobSpec _jobSpec;
  private final JobExecutionUpdatable _jobExec;
  private final JobExecutionState _jobState;
  private final JobExecutionStateListeners _callbackDispatcher;
  private final ExecutionList _executionList;
  private final DriverRunnable _runnable;
  private final Closer _closer;
  private JobContext _jobContext;

  /**
   * Creates a new JobExecutionDriver which acts as an adapter to the legacy {@link JobLauncher} API.
   * @param sysConfig             the system/environment config
   * @param jobSpec               the JobSpec to be executed
   * @param jobLauncherType       an optional jobLauncher type; the value follows the convention of
   *        {@link JobLauncherFactory#newJobLauncher(java.util.Properties, java.util.Properties, String).
   *        If absent, {@link JobLauncherFactory#newJobLauncher(java.util.Properties, java.util.Properties)}
   *        will be used which looks for the {@link ConfigurationKeys#JOB_LAUNCHER_TYPE_KEY}
   *        in the system configuration.
   * @param jobExecStateListener  an optional listener to listen for state changes in the execution.
   * @param log                   an optional logger to be used; if none is specified, a default one
   *                              will be instantiated.
   */
  public static JobLauncherExecutionDriver create(Configurable sysConfig, JobSpec jobSpec,
      Optional<JobLauncherFactory.JobLauncherType> jobLauncherType,
      Optional<Logger> log, boolean instrumentationEnabled,
      JobExecutionLauncher.StandardMetrics launcherMetrics, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) {

    Logger actualLog = log.isPresent() ? log.get() : LoggerFactory.getLogger(JobLauncherExecutionDriver.class);

    JobExecutionStateListeners callbackDispatcher = new JobExecutionStateListeners(actualLog);
    JobExecutionUpdatable jobExec = JobExecutionUpdatable.createFromJobSpec(jobSpec);
    JobExecutionState jobState = new JobExecutionState(jobSpec, jobExec,
        Optional.<JobExecutionStateListener>of(callbackDispatcher));

    JobLauncher jobLauncher = createLauncher(sysConfig, jobSpec, actualLog, jobLauncherType.isPresent() ?
        Optional.of(jobLauncherType.get().toString()) :
        Optional.<String>absent(), instanceBroker);
    JobListenerToJobStateBridge bridge = new JobListenerToJobStateBridge(actualLog, jobState, instrumentationEnabled, launcherMetrics);

    DriverRunnable runnable = new DriverRunnable(jobLauncher, bridge, jobState, callbackDispatcher, jobExec);

    return new JobLauncherExecutionDriver(jobSpec, actualLog, runnable);
  }

  protected JobLauncherExecutionDriver(JobSpec jobSpec, Logger log, DriverRunnable runnable) {
    super(runnable);
    _closer = Closer.create();
    _closer.register(runnable.getJobLauncher());
    _log = log;
    _jobSpec = jobSpec;
    _jobExec = runnable.getJobExec();
    _callbackDispatcher = _closer.register(runnable.getCallbackDispatcher());
    _jobState = runnable.getJobState();
    _executionList = new ExecutionList();
    _runnable = runnable;
  }

  /**
   * A runnable that actually executes the job.
   */
  @AllArgsConstructor
  @Getter
  private static class DriverRunnable implements Callable<JobExecutionResult> {

    private final JobLauncher jobLauncher;
    private final JobListenerToJobStateBridge bridge;
    private final JobExecutionState jobState;
    private final JobExecutionStateListeners callbackDispatcher;
    private final JobExecutionUpdatable jobExec;

    @Override
    public JobExecutionResult call() throws JobException, InterruptedException, TimeoutException  {
        jobLauncher.launchJob(bridge);
        jobState.awaitForDone(Long.MAX_VALUE);
        return JobExecutionResult.createFromState(jobState);
    }
  }

  private static JobLauncher createLauncher(Configurable _sysConfig, JobSpec _jobSpec, Logger _log,
      Optional<String> jobLauncherType, SharedResourcesBroker<GobblinScopeTypes> instanceBroker) {
    if (jobLauncherType.isPresent()) {
      return JobLauncherFactory.newJobLauncher(_sysConfig.getConfigAsProperties(),
             _jobSpec.getConfigAsProperties(), jobLauncherType.get(), instanceBroker);
    }
    else {
      _log.info("Creating auto jobLauncher for " + _jobSpec);
      try {
        return JobLauncherFactory.newJobLauncher(_sysConfig.getConfigAsProperties(),
             _jobSpec.getConfigAsProperties(), instanceBroker);
      } catch (Exception e) {
        throw new RuntimeException("JobLauncher creation failed: " + e, e);
      }
    }
  }

  @Override
  public JobExecution getJobExecution() {
    return _jobExec;
  }

  @Override
  public JobExecutionStatus getJobExecutionStatus() {
    return _jobState;
  }

  protected void startAsync() throws JobException {
    _log.info("Starting " + getClass().getSimpleName());
    ExecutorsUtils.newThreadFactory(Optional.of(_log), Optional.of("job-launcher-execution-driver")).newThread(this).start();
  }

  @Override
  protected void done() {
    _executionList.execute();
    try {
      shutDown();
    } catch (IOException ioe) {
      _log.error("Failed to close job launcher.");
    }
  }

  private void shutDown() throws IOException {
    _log.info("Shutting down " + getClass().getSimpleName());
    if (null != _jobContext) {
      switch (_jobContext.getJobState().getState()) {
        case PENDING:
        case SUCCESSFUL:
        case RUNNING: {
          // We have to pass another listener instance as launcher does not store the listener used
          // in launchJob()
          cancel(false);
          break;
        }
        case FAILED:
        case COMMITTED:
        case CANCELLED: {
          // Nothing to do
          break;
        }
      }
    }

    _closer.close();
  }

  @Override
  public void addListener(Runnable listener, Executor executor) {
    _executionList.add(listener, executor);
  }

  static class JobListenerToJobStateBridge extends AbstractJobListener {

    private final JobExecutionState _jobState;
    private final boolean _instrumentationEnabled;
    private final JobExecutionLauncher.StandardMetrics _launcherMetrics;

    private JobContext _jobContext;

    public JobListenerToJobStateBridge(Logger log, JobExecutionState jobState,
        boolean instrumentationEnabled, JobExecutionLauncher.StandardMetrics launcherMetrics) {
      super(Optional.of(log));
      _jobState = jobState;
      _instrumentationEnabled = instrumentationEnabled;
      _launcherMetrics = launcherMetrics;
    }


    @Override
    public void onJobPrepare(JobContext jobContext) throws Exception {
      super.onJobPrepare(jobContext);
      _jobContext = jobContext;
      if (_jobState.getRunningState() == null) {
        _jobState.switchToPending();
      }
      _jobState.switchToRunning();
      if (_instrumentationEnabled && null != _launcherMetrics) {
        _launcherMetrics.getNumJobsLaunched().inc();
      }
    }

    @Override
    public void onJobStart(JobContext jobContext) throws Exception {
      super.onJobStart(jobContext);
    }

    @Override
    public void onJobCompletion(JobContext jobContext) throws Exception {
      Preconditions.checkArgument(jobContext.getJobState().getState() == RunningState.SUCCESSFUL
          || jobContext.getJobState().getState() == RunningState.COMMITTED
          || jobContext.getJobState().getState() == RunningState.FAILED,
          "Unexpected state: " + jobContext.getJobState().getState() + " in " + jobContext);
      super.onJobCompletion(jobContext);
      if (_instrumentationEnabled && null != _launcherMetrics) {
        _launcherMetrics.getNumJobsCompleted().inc();
      }
      if (jobContext.getJobState().getState() == RunningState.FAILED) {
        if (_instrumentationEnabled && null != _launcherMetrics) {
          _launcherMetrics.getNumJobsFailed().inc();
        }
        _jobState.switchToFailed();
      }
      else {
        // TODO Remove next line once the JobLauncher starts sending notifications for success
        _jobState.switchToSuccessful();
        _jobState.switchToCommitted();
        if (_instrumentationEnabled && null != _launcherMetrics) {
          _launcherMetrics.getNumJobsCommitted().inc();
        }
      }
    }

    @Override
    public void onJobCancellation(JobContext jobContext) throws Exception {
      super.onJobCancellation(jobContext);
      _jobState.switchToCancelled();
      if (_instrumentationEnabled && null != _launcherMetrics) {
        _launcherMetrics.getNumJobsCancelled().inc();
      }
    }
  }

  @VisibleForTesting JobLauncher getLegacyLauncher() {
    return _runnable.getJobLauncher();
  }

  /** {@inheritDoc} */
  @Override public void registerStateListener(JobExecutionStateListener listener) {
    _callbackDispatcher.registerStateListener(listener);
  }

  /** {@inheritDoc} */
  @Override public void unregisterStateListener(JobExecutionStateListener listener) {
    _callbackDispatcher.unregisterStateListener(listener);
  }

  /** {@inheritDoc} */
  @Override public JobExecutionState getJobExecutionState() {
    return _jobState;
  }

  /**
   * Creates a new instance of {@link JobLauncherExecutionDriver}.
   *
   * <p>Conventions
   * <ul>
   *  <li>If no jobLauncherType is specified, one will be determined by the JobSpec
   *  (see {@link JobLauncherFactory).
   *  <li> Convention for sysConfig: use the sysConfig of the gobblinInstance if specified,
   *       otherwise use empty config.
   *  <li> Convention for log: use gobblinInstance logger plus "." + jobSpec if specified, otherwise
   *       use JobExecutionDriver class name plus "." + jobSpec
   * </ul>
   */
  public static class Launcher implements JobExecutionLauncher, GobblinInstanceEnvironment {
    private Optional<JobLauncherType> _jobLauncherType = Optional.absent();
    private Optional<Configurable> _sysConfig = Optional.absent();
    private Optional<GobblinInstanceEnvironment> _gobblinEnv = Optional.absent();
    private Optional<Logger> _log = Optional.absent();
    private Optional<MetricContext> _metricContext = Optional.absent();
    private Optional<Boolean> _instrumentationEnabled = Optional.absent();
    private JobExecutionLauncher.StandardMetrics _metrics;
    private Optional<SharedResourcesBroker<GobblinScopeTypes>> _instanceBroker = Optional.absent();

    public Launcher() {
    }

    /** Leave unchanged for */
    public Launcher withJobLauncherType(JobLauncherType jobLauncherType) {
      Preconditions.checkNotNull(jobLauncherType);
      _jobLauncherType = Optional.of(jobLauncherType);
      return this;
    }

    public Optional<JobLauncherType> getJobLauncherType() {
      return _jobLauncherType;
    }

    /** System-wide settings */
    public Configurable getDefaultSysConfig() {
      return _gobblinEnv.isPresent() ?
          _gobblinEnv.get().getSysConfig() :
          DefaultConfigurableImpl.createFromConfig(ConfigFactory.empty());
    }

    @Override
    public Configurable getSysConfig() {
      if (!_sysConfig.isPresent()) {
        _sysConfig = Optional.of(getDefaultSysConfig());
      }
      return _sysConfig.get();
    }

    public Launcher withSysConfig(Configurable sysConfig) {
      _sysConfig = Optional.of(sysConfig);
      return this;
    }

    /** Parent Gobblin instance */
    public Launcher withGobblinInstanceEnvironment(GobblinInstanceEnvironment gobblinInstance) {
      _gobblinEnv = Optional.of(gobblinInstance);
      return this;
    }

    public Optional<GobblinInstanceEnvironment> getGobblinInstanceEnvironment() {
      return _gobblinEnv;
    }

    public Logger getLog(JobSpec jobSpec) {
      return getJobLogger(getLog(), jobSpec);
    }

    public Launcher withInstrumentationEnabled(boolean enabled) {
      _instrumentationEnabled = Optional.of(enabled);
      return this;
    }

    public boolean getDefaultInstrumentationEnabled() {
      return _gobblinEnv.isPresent() ? _gobblinEnv.get().isInstrumentationEnabled() :
          GobblinMetrics.isEnabled(getSysConfig().getConfig());
    }

    @Override
    public boolean isInstrumentationEnabled() {
      if (!_instrumentationEnabled.isPresent()) {
        _instrumentationEnabled = Optional.of(getDefaultInstrumentationEnabled());
      }
      return _instrumentationEnabled.get();
    }

    private static Logger getJobLogger(Logger parentLog, JobSpec jobSpec) {
      return LoggerFactory.getLogger(parentLog.getName() + "." + jobSpec.toShortString());
    }

    public Launcher withMetricContext(MetricContext instanceMetricContext) {
      _metricContext = Optional.of(instanceMetricContext);
      return this;
    }

    @Override
    public MetricContext getMetricContext() {
      if (!_metricContext.isPresent()) {
        _metricContext = Optional.of(getDefaultMetricContext());
      }
      return _metricContext.get();
    }

    public MetricContext getDefaultMetricContext() {
      if (_gobblinEnv.isPresent()) {
        return _gobblinEnv.get().getMetricContext()
            .childBuilder(JobExecutionLauncher.class.getSimpleName()).build();
      }
      gobblin.configuration.State fakeState =
          new gobblin.configuration.State(getSysConfig().getConfigAsProperties());
      List<Tag<?>> tags = new ArrayList<>();
      MetricContext res = Instrumented.getMetricContext(fakeState, Launcher.class, tags);
      return res;
    }

    @Override public JobExecutionDriver launchJob(JobSpec jobSpec) {
      Preconditions.checkNotNull(jobSpec);
      if (!(jobSpec instanceof ResolvedJobSpec)) {
        try {
          jobSpec = new ResolvedJobSpec(jobSpec);
        } catch (JobTemplate.TemplateException | SpecNotFoundException exc) {
          throw new RuntimeException("Can't launch job " + jobSpec.getUri(), exc);
        }
      }
      return JobLauncherExecutionDriver.create(getSysConfig(), jobSpec, _jobLauncherType,
          Optional.of(getLog(jobSpec)), isInstrumentationEnabled(), getMetrics(), getInstanceBroker());
    }

    @Override public List<Tag<?>> generateTags(gobblin.configuration.State state) {
      return Collections.emptyList();
    }

    @Override public void switchMetricContext(List<Tag<?>> tags) {
      throw new UnsupportedOperationException();
    }

    @Override public void switchMetricContext(MetricContext context) {
      throw new UnsupportedOperationException();
    }

    @Override public String getInstanceName() {
      return _gobblinEnv.isPresent() ? _gobblinEnv.get().getInstanceName() : getClass().getName();
    }

    public Logger getDefaultLog() {
      return _gobblinEnv.isPresent() ? _gobblinEnv.get().getLog() : LoggerFactory.getLogger(getClass());
    }

    @Override public Logger getLog() {
      if (! _log.isPresent()) {
        _log = Optional.of(getDefaultLog());
      }
      return _log.get();
    }

    public Launcher withLog(Logger log) {
      _log = Optional.of(log);
      return this;
    }

    @Override public StandardMetrics getMetrics() {
      if (_metrics == null) {
        _metrics = new JobExecutionLauncher.StandardMetrics(this);
      }
      return _metrics;
    }

    public Launcher withInstanceBroker(SharedResourcesBroker<GobblinScopeTypes> broker) {
      _instanceBroker = Optional.of(broker);
      return this;
    }

    public SharedResourcesBroker<GobblinScopeTypes> getInstanceBroker() {
      if (!_instanceBroker.isPresent()) {
        if (_gobblinEnv.isPresent()) {
          _instanceBroker = Optional.of(_gobblinEnv.get().getInstanceBroker());
        } else {
          _instanceBroker = Optional.of(getDefaultInstanceBroker());
        }
      }
      return _instanceBroker.get();
    }

    public SharedResourcesBroker<GobblinScopeTypes> getDefaultInstanceBroker() {
      getLog().warn("Creating a default instance broker for job launcher. Objects may not be shared across all jobs in this instance.");
      SharedResourcesBrokerImpl<GobblinScopeTypes> globalBroker =
          SharedResourcesBrokerFactory.createDefaultTopLevelBroker(getSysConfig().getConfig(),
              GobblinScopeTypes.GLOBAL.defaultScopeInstance());
      return globalBroker.newSubscopedBuilder(new SimpleScope<>(GobblinScopeTypes.INSTANCE, getInstanceName())).build();
    }

  }

  @Override public void registerWeakStateListener(JobExecutionStateListener listener) {
    _callbackDispatcher.registerWeakStateListener(listener);
  }

  @Override public boolean isDone() {
    RunningState runState = getJobExecutionStatus().getRunningState();
    return runState == null ? false : runState.isDone() ;
  }

  @Override public boolean cancel(boolean mayInterruptIfRunning) {
    // FIXME there is a race condition here as the job may complete successfully before we
    // call cancelJob() below. There isn't an easy way to fix that right now.

    RunningState runState = getJobExecutionStatus().getRunningState();
    if (runState.isCancelled()) {
      return true;
    }
    else if (runState.isDone()) {
      return false;
    }
    try {
      // No special processing of callbacks necessary
      getLegacyLauncher().cancelJob(new AbstractJobListener(){});
    } catch (JobException e) {
      throw new RuntimeException("Unable to cancel job " + _jobSpec + ": " + e, e);
    }
    return super.cancel(mayInterruptIfRunning);
  }

  @Override public boolean isCancelled() {
    return getJobExecutionStatus().getRunningState().isCancelled();
  }

  @Override
  public JobExecutionResult get()
      throws InterruptedException {
    try {
      return super.get();
    } catch (ExecutionException ee) {
      return JobExecutionResult.createFailureResult(ee.getCause());
    }
  }

  @Override
  public JobExecutionResult get(long timeout, TimeUnit unit)
      throws InterruptedException, TimeoutException {
    try {
      return super.get(timeout, unit);
    } catch (ExecutionException ee) {
      return JobExecutionResult.createFailureResult(ee.getCause());
    }
  }
}
