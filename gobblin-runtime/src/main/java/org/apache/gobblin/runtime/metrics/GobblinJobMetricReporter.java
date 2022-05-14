package org.apache.gobblin.runtime.metrics;

import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;


public interface GobblinJobMetricReporter {

   void reportWorkUnitCreationTimerMetrics(TimingEvent workUnitsCreationTimer, JobState jobState);

   void reportWorkUnitCountMetrics(int workUnitCount, JobState jobState);

}
