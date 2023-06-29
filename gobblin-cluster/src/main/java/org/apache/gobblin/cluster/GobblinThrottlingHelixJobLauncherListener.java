package org.apache.gobblin.cluster;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.JobContext;
import org.apache.gobblin.runtime.JobState;


/**
 * A job listener used when {@link GobblinHelixJobLauncher} launches a job.
 * In {@link GobblinHelixJobScheduler}, when throttling is enabled, this
 * listener would record jobName to next schedulable time to decide whether
 * the replanning should be executed or skipped.
 */
@Slf4j
public class GobblinThrottlingHelixJobLauncherListener extends GobblinHelixJobLauncherListener {

  public final static Logger LOG = LoggerFactory.getLogger(GobblinThrottlingHelixJobLauncherListener.class);
  private ConcurrentHashMap<String, Instant> jobNameToNextSchedulableTime;

  public GobblinThrottlingHelixJobLauncherListener(GobblinHelixJobLauncherMetrics jobLauncherMetrics,
      ConcurrentHashMap<String, Instant> jobNameToNextSchedulableTime) {
    super(jobLauncherMetrics);
    this.jobNameToNextSchedulableTime = jobNameToNextSchedulableTime;
  }

  @Override
  public void onJobCompletion(JobContext jobContext)
      throws Exception {
    super.onJobCompletion(jobContext);
    if (jobContext.getJobState().getState() == JobState.RunningState.FAILED) {
      jobNameToNextSchedulableTime.put(jobContext.getJobName(), Instant.EPOCH);
      LOG.info("{} failed. The next schedulable time is {} so that any future schedule attempts will be allowed.",
          jobContext.getJobName(),
          Instant.EPOCH);
    }
  }

  @Override
  public void onJobCancellation(JobContext jobContext)
      throws Exception {
    super.onJobCancellation(jobContext);
    jobNameToNextSchedulableTime.put(jobContext.getJobName(), Instant.EPOCH);
    LOG.info("{} is cancelled. The next schedulable time is {} so that any future schedule attempts will be allowed.",
        jobContext.getJobName(),
        Instant.EPOCH);
  }
}
