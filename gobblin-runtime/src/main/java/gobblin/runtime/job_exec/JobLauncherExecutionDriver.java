package gobblin.runtime.job_exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;

import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.JobContext;
import gobblin.runtime.JobLauncher;
import gobblin.runtime.JobLauncherFactory;
import gobblin.runtime.JobState;
import gobblin.runtime.api.Configurable;
import gobblin.runtime.api.JobExecution;
import gobblin.runtime.api.JobExecutionDriver;
import gobblin.runtime.api.JobExecutionStatus;
import gobblin.runtime.api.JobSpec;
import gobblin.runtime.listeners.AbstractJobListener;
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
  private final JobExecutionStatus _jobStatus;
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
      Optional<Logger> log) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    _sysConfig = sysConfig;
    _jobSpec = jobSpec;
    _jobExec = JobExecutionUpdatable.createFromJobSpec(jobSpec);
    _jobStatus = new JobExecutionStatus(_jobExec);
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
    return _jobStatus;
  }

  @Override
  protected void startUp() throws Exception {
    _log.info("Starting " + getClass().getSimpleName());
    _legacyLauncher.launchJob(new MyJobListener());
  }

  @Override
  protected void shutDown() throws Exception {
    _log.info("Shutting down " + getClass().getSimpleName());
    if (null != _jobContext) {
      switch (_jobContext.getJobState().getState()) {
        case PENDING:
        case RUNNING: {
          // We have to pass another listener instance as launcher does not store the listener used
          // in launchJob()
          _legacyLauncher.cancelJob(new MyJobListener());
          break;
        }
        case SUCCESSFUL:
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

  class MyJobListener extends AbstractJobListener {

    public MyJobListener() {
      super(Optional.of(JobLauncherExecutionDriver.this._log));
    }

    @Override
    public void onJobPrepare(JobContext jobContext) throws Exception {
      super.onJobPrepare(jobContext);
      _jobContext = jobContext;
      _jobStatus.setStatus(JobState.RunningState.PENDING);
    }

    @Override
    public void onJobStart(JobContext jobContext) throws Exception {
      super.onJobStart(jobContext);
      _jobStatus.setStatus(jobContext.getJobState().getState());
    }

    @Override
    public void onJobCompletion(JobContext jobContext) throws Exception {
      super.onJobCompletion(jobContext);
      _jobStatus.setStatus(jobContext.getJobState().getState());
    }

    @Override
    public void onJobCancellation(JobContext jobContext) throws Exception {
      super.onJobCancellation(jobContext);
      _jobStatus.setStatus(JobState.RunningState.CANCELLED);
    }

    // FIXME CRITICAL Currently, we can't detect that the transition RUNNING -> SUCCESSFUL as
    // there is no notification. That transition happens internally in JobContext.

  }

  @VisibleForTesting
  JobLauncher getLegacyLauncher() {
    return _legacyLauncher;
  }

}
