package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.sql.Timestamp;


public interface SchedulerLeaseDeterminationStore {

  /**
   *
   * @param flowGroup
   * @param flowName
   * @param flowExecutionId
   * @param triggerTimestamp
   * @return True if obtained lease and completed insert, False otherwise
   */
  public boolean attemptLeaseOfLaunchEvent(String flowGroup, String flowName, String flowExecutionId,
      Timestamp triggerTimestamp) throws IOException;

  public Timestamp getPursuantTimestamp(String flowGroup, String flowName, String flowExecutionId,
      Timestamp triggerTimestamp) throws IOException;

}
