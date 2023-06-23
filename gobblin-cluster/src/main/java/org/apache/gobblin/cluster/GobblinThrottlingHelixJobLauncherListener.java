package org.apache.gobblin.cluster;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;


/**
 * A job listener used when {@link GobblinHelixJobLauncher} launches a job.
 * In {@link GobblinHelixJobScheduler}, when throttling is enabled, this
 * listener would record jobName to next schedulable time to decide whether
 * the replanning should be executed or skipped.
 */
public class GobblinThrottlingHelixJobLauncherListener extends GobblinHelixJobLauncherListener {

  public final static Logger LOG = LoggerFactory.getLogger(GobblinThrottlingHelixJobLauncherListener.class);
  private ConcurrentHashMap<String, Instant> jobNameToNextSchedulableTime;
  private Duration helixJobSchedulingThrottleTimeout;
  private Clock clock;

  public GobblinThrottlingHelixJobLauncherListener(GobblinHelixJobLauncherMetrics jobLauncherMetrics,
      ConcurrentHashMap jobNameToNextSchedulableTime, Duration helixJobSchedulingThrottleTimeout, Clock clock) {
    super(jobLauncherMetrics);
    this.jobNameToNextSchedulableTime = jobNameToNextSchedulableTime;
    this.helixJobSchedulingThrottleTimeout = helixJobSchedulingThrottleTimeout;
    this.clock = clock;
  }

  @Override
  public void onJobPrepare(JobContext jobContext)
      throws Exception {
    super.onJobPrepare(jobContext);
    Instant nextSchedulableTime = clock.instant().plus(helixJobSchedulingThrottleTimeout);
    jobNameToNextSchedulableTime.put(jobContext.getJobName(), nextSchedulableTime);
    LOG.info(jobContext.getJobName() + " finished prepare. The next schedulable time is  " + nextSchedulableTime );
  }

  @Override
  public void onJobCompletion(JobContext jobContext)
      throws Exception {
    super.onJobCompletion(jobContext);
    if (jobContext.getJobState().getState() == JobState.RunningState.FAILED) {
      jobNameToNextSchedulableTime.put(jobContext.getJobName(), Instant.ofEpochMilli(0));
    } else {
      Instant nextSchedulableTime = clock.instant().plus(helixJobSchedulingThrottleTimeout);
      jobNameToNextSchedulableTime.put(jobContext.getJobName(), nextSchedulableTime);
      LOG.info(jobContext.getJobName() + " finished completion. The next schedulable time is " + nextSchedulableTime );
    }
  }

  @Override
  public void onJobCancellation(JobContext jobContext)
      throws Exception {
    super.onJobCancellation(jobContext);
    jobNameToNextSchedulableTime.put(jobContext.getJobName(), Instant.ofEpochMilli(0));
  }
}
