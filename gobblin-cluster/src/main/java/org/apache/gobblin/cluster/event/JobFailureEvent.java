package org.apache.gobblin.cluster.event;

import lombok.Getter;
import org.apache.gobblin.runtime.JobState;


/**
 * The `JobFailureEvent` class represents an event that is triggered when a job fails.
 * It contains information about the job state and a summary of the issues that caused the failure.
 */
public class JobFailureEvent {
  @Getter
  private final JobState jobState;
  @Getter
  private final String issuesSummary;
  public JobFailureEvent(JobState jobState, String issuesSummary) {
    this.jobState = jobState;
    this.issuesSummary = issuesSummary;
  }
}
