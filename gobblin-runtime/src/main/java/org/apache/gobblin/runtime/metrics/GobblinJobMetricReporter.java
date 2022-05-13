package org.apache.gobblin.runtime.metrics;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;


public interface GobblinJobMetricReporter {

  public void reportWorkUnitCreationTimerMetrics(TimingEvent workUnitsCreationTimer, JobState jobState);

  public void reportWorkUnitCountMetrics(int workUnitCount, JobState jobState);

}
