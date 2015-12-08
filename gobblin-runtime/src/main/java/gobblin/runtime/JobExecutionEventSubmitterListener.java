package gobblin.runtime;

import lombok.AllArgsConstructor;


@AllArgsConstructor

/**
 * Implementation of {@link JobListener} that submits metadata events via {@link JobExecutionEventSubmitter} when a job
 * is completed or is cancelled.
 */
public class JobExecutionEventSubmitterListener implements JobListener {

  private final JobExecutionEventSubmitter jobExecutionEventSubmitter;

  @Override
  public void onJobCompletion(JobState jobState) {
    this.jobExecutionEventSubmitter.submitJobExecutionEvents(jobState);
  }

  @Override
  public void onJobCancellation(JobState jobState) {
    this.jobExecutionEventSubmitter.submitJobExecutionEvents(jobState);
  }
}
