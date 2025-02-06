package org.apache.gobblin.cluster.event;

import lombok.Getter;
import org.apache.gobblin.runtime.JobState;


/*

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
