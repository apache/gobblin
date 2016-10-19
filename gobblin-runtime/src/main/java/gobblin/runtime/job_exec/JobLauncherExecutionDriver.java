package gobblin.runtime.job_exec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.ConfigFactory;

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
import gobblin.runtime.listeners.AbstractJobListener;
import gobblin.runtime.std.DefaultConfigurableImpl;
import gobblin.runtime.std.JobExecutionStateListeners;
import gobblin.runtime.std.JobExecutionUpdatable;

/**
 * An implementation of JobExecutionDriver which acts as an adapter to the legacy
 * {@link JobLauncher} API.
 */
public class JobLauncherExecutionDriver extends AbstractIdleService implements JobExecutionDriver {
  private final Logger _log;
  private final Configurable _sysConfig;
  private final JobLauncher _legacyLauncher;
  private final JobSpec _jobSpec;
  private final JobExecutionUpdatable _jobExec;
  private final JobExecutionState _jobState;
  private final JobExecutionStateListeners _callbackDispatcher;
  private final boolean _instrumentationEnabled;
  private final JobExecutionLauncher.StandardMetrics _launcherMetrics;
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
  public JobLauncherExecutionDriver(Configurable sysConfig, JobSpec jobSpec,
      Optional<JobLauncherFactory.JobLauncherType> jobLauncherType,
      Optional<Logger> log, boolean instrumentationEnabled,
      JobExecutionLauncher.StandardMetrics launcherMetrics) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _sysConfig = sysConfig;
    _jobSpec = jobSpec;
    _jobExec = JobExecutionUpdatable.createFromJobSpec(jobSpec);
    _callbackDispatcher = new JobExecutionStateListeners(_log);
    _jobState = new JobExecutionState(_jobSpec, _jobExec,
                                      Optional.<JobExecutionStateListener>of(_callbackDispatcher));
    _instrumentationEnabled = instrumentationEnabled;
    _launcherMetrics = launcherMetrics;
    _legacyLauncher =
        createLauncher(jobLauncherType.isPresent() ?
                      Optional.of(jobLauncherType.get().toString()) :
                      Optional.<String>absent());
  }

  private JobLauncher createLauncher(Optional<String> jobLauncherType) {
    if (jobLauncherType.isPresent()) {
      return JobLauncherFactory.newJobLauncher(_sysConfig.getConfigAsProperties(),
             _jobSpec.getConfigAsProperties(), jobLauncherType.get());
    }
    else {
      _log.info("Creating auto jobLauncher for " + _jobSpec);
      try {
        return JobLauncherFactory.newJobLauncher(_sysConfig.getConfigAsProperties(),
             _jobSpec.getConfigAsProperties());
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

  @Override
  protected void startUp() throws Exception {
    _log.info("Starting " + getClass().getSimpleName());
    _legacyLauncher.launchJob(new JobListenerToJobStateBridge());
  }

  @Override
  protected void shutDown() throws Exception {
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

    _legacyLauncher.close();
  }


  class JobListenerToJobStateBridge extends AbstractJobListener {

    public JobListenerToJobStateBridge() {
      super(Optional.of(JobLauncherExecutionDriver.this._log));
    }

    @Override
    public void onJobPrepare(JobContext jobContext) throws Exception {
      super.onJobPrepare(jobContext);
      _jobContext = jobContext;
      _jobState.switchToPending();
      if (_instrumentationEnabled && null != _launcherMetrics) {
        _launcherMetrics.getNumJobsLaunched().inc();
      }
    }

    @Override
    public void onJobStart(JobContext jobContext) throws Exception {
      super.onJobStart(jobContext);
      _jobState.switchToRunning();
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
    return _legacyLauncher;
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

    public Launcher() {
      _metrics = new JobExecutionLauncher.StandardMetrics(this);
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
      return new JobLauncherExecutionDriver(getSysConfig(), jobSpec, _jobLauncherType,
          Optional.of(getLog(jobSpec)), isInstrumentationEnabled(), getMetrics());
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
      return _metrics;
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
      _legacyLauncher.cancelJob(new AbstractJobListener(){});
    } catch (JobException e) {
      throw new RuntimeException("Unable to cancel job " + _jobSpec + ": " + e, e);
    }
    return true;
  }

  @Override public boolean isCancelled() {
    return getJobExecutionStatus().getRunningState().isCancelled();
  }

  @Override public JobExecutionResult get() throws InterruptedException {
    try {
      return get(0, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      throw new Error("This should never happen.");
    }
  }

  @Override
  public JobExecutionResult get(long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
    Preconditions.checkNotNull(unit);
    if (0 == timeout) {
      timeout = Long.MAX_VALUE;
      unit = TimeUnit.SECONDS;
    }
    getJobExecutionState().awaitForDone(unit.toMillis(timeout));
    return JobExecutionResult.createFromState(getJobExecutionState());
  }
}
