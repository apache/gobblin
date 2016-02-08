package gobblin.runtime.listeners;

import lombok.AllArgsConstructor;

import gobblin.runtime.JobContext;
import gobblin.runtime.JobExecutionEventSubmitter;


@AllArgsConstructor

/**
 * Implementation of {@link JobListener} that submits metadata events via {@link JobExecutionEventSubmitter} when a job
 * is completed or is cancelled.
 */
public class JobExecutionEventSubmitterListener extends AbstractJobListener {

  private final JobExecutionEventSubmitter jobExecutionEventSubmitter;

  @Override
  public void onJobCompletion(JobContext jobContext) {
    this.jobExecutionEventSubmitter.submitJobExecutionEvents(jobContext.getJobState());
  }

  @Override
  public void onJobCancellation(JobContext jobContext) {
    this.jobExecutionEventSubmitter.submitJobExecutionEvents(jobContext.getJobState());
  }
}
