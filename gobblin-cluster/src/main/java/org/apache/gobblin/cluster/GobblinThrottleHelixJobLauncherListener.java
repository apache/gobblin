package org.apache.gobblin.cluster;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;


public class GobblinThrottleHelixJobLauncherListener extends GobblinHelixJobLauncherListener {

  public final static Logger LOG = LoggerFactory.getLogger(GobblinThrottleHelixJobLauncherListener.class);
  private ConcurrentHashMap<String, Instant> jobNameToNextSchedulableTime;
  private Duration helixJobSchedulingThrottleTimeout;
  private Clock clock;

  GobblinThrottleHelixJobLauncherListener(GobblinHelixJobLauncherMetrics jobLauncherMetrics,
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
    Instant finishTime = clock.instant().plus(helixJobSchedulingThrottleTimeout);
    jobNameToNextSchedulableTime.put(jobContext.getJobName(), finishTime);
    LOG.info(jobContext.getJobName() + " finishes onJobPrepare at " + finishTime );
  }

  @Override
  public void onJobCompletion(JobContext jobContext)
      throws Exception {
    super.onJobCompletion(jobContext);
    if (jobContext.getJobState().getState() == JobState.RunningState.FAILED) {
      jobNameToNextSchedulableTime.put(jobContext.getJobName(), Instant.ofEpochMilli(0));
    } else {
      Instant finishTime = clock.instant().plus(helixJobSchedulingThrottleTimeout);
      jobNameToNextSchedulableTime.put(jobContext.getJobName(), finishTime);
      LOG.info(jobContext.getJobName() + " finishes onJobCompletion at " + finishTime );
    }
  }

  @Override
  public void onJobCancellation(JobContext jobContext)
      throws Exception {
    super.onJobCancellation(jobContext);
    jobNameToNextSchedulableTime.put(jobContext.getJobName(), Instant.ofEpochMilli(0));
  }
}
