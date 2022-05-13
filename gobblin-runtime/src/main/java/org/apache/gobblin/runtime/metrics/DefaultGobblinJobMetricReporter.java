package org.apache.gobblin.runtime.metrics;

import com.codahale.metrics.MetricRegistry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.JobState;


/**
 * A metrics reporter that reports only workunitsCreationTimer - which is the current default behavior
 *
 */
public class DefaultGobblinJobMetricReporter implements GobblinJobMetricReporter {

  private Optional<MetricContext> metricContext;

  public DefaultGobblinJobMetricReporter(Optional<MetricContext> metricContext) {
     this.metricContext = metricContext;
  }

  public void reportWorkUnitCreationTimerMetrics(TimingEvent workUnitsCreationTimer, JobState jobState) {
    if (!this.metricContext.isPresent()) {
      return;
    }
    String workunitCreationGaugeName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_JOB_METRICS_PREFIX,
        TimingEvent.LauncherTimings.WORK_UNITS_CREATION, jobState.getJobName());
    long workUnitsCreationTime = workUnitsCreationTimer.getDuration() / TimeUnit.SECONDS.toMillis(1);
    ContextAwareGauge<Integer> workunitCreationGauge = this.metricContext.get()
        .newContextAwareGauge(workunitCreationGaugeName, () -> (int) workUnitsCreationTime);
    this.metricContext.get().register(workunitCreationGaugeName, workunitCreationGauge);
  }

  public void reportWorkUnitCountMetrics(int workUnitCount, JobState jobstate) {
    return;
  }

}
