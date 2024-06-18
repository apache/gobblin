package org.apache.gobblin.service.modules.orchestration.task;

import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ConcurrentMap;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.metrics.ServiceMetricNames;

/**
 * Used to track all metrics relating to processing dagActions when DagProcessingEngine is enabled. The metrics can be
 * used to trace the number of dagActions (which can be further broken down by time) at various points of the system,
 * starting from addition to the DagActionStore to observation in the DagActionChangeMonitor through the
 * DagProcessingEngine pipeline (DagManagement -> DagTaskStreamImpl -> MySqlMultiActiveLeaseArbiter -> DagProc).
 */
public class DagProcessingEngineMetrics {
  MetricContext metricContext;
  // Declare map of dagActionType to a ContextAwareMeter for each metric
  private ConcurrentMap<String, ContextAwareMeter> dagActionsStoredMeters = new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsObservedMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsLeasesObtainedMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsNoLongerLeasingMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionLeaseRemindersScheduledMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionRemindersProcessedMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsExceededMaxRetryMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsInitFailedMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsInitSucceededMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionExecutionsFailedMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionExecutionsSucceededMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionConclusionsFailedMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionConclusionsSucceededMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsRemovedFromStoreMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionsFailingRemovalMeters =  new ConcurrentHashMap();
  private ConcurrentMap<String, ContextAwareMeter>dagActionAverageProcessingDelayMillisMeters =  new ConcurrentHashMap();

  public DagProcessingEngineMetrics(MetricContext metricContext) {
    this.metricContext = metricContext;
    registerAllMetrics();
  }

  public void registerAllMetrics() {
    registerMetricForEachDagAction(this.dagActionsStoredMeters, ServiceMetricNames.DAG_ACTIONS_STORED);
    registerMetricForEachDagAction(this.dagActionsObservedMeters, ServiceMetricNames.DAG_ACTIONS_OBSERVED);
    registerMetricForEachDagAction(this.dagActionsLeasesObtainedMeters, ServiceMetricNames.DAG_ACTIONS_LEASES_OBTAINED);
    registerMetricForEachDagAction(this.dagActionsNoLongerLeasingMeters, ServiceMetricNames.DAG_ACTIONS_NO_LONGER_LEASING);
    registerMetricForEachDagAction(this.dagActionLeaseRemindersScheduledMeters, ServiceMetricNames.DAG_ACTION_LEASE_REMINDERS_SCHEDULED);
    registerMetricForEachDagAction(this.dagActionRemindersProcessedMeters, ServiceMetricNames.DAG_ACTION_REMINDERS_PROCESSED);
    registerMetricForEachDagAction(this.dagActionsExceededMaxRetryMeters, ServiceMetricNames.DAG_ACTIONS_EXCEEDED_MAX_RETRY);
    registerMetricForEachDagAction(this.dagActionsInitFailedMeters, ServiceMetricNames.DAG_ACTIONS_INIT_FAILED);
    registerMetricForEachDagAction(this.dagActionsInitSucceededMeters, ServiceMetricNames.DAG_ACTIONS_INIT_SUCCEEDED);
    registerMetricForEachDagAction(this.dagActionExecutionsFailedMeters, ServiceMetricNames.DAG_ACTION_EXECUTIONS_FAILED);
    registerMetricForEachDagAction(this.dagActionExecutionsSucceededMeters, ServiceMetricNames.DAG_ACTION_EXECUTIONS_SUCCEEDED);
    registerMetricForEachDagAction(this.dagActionConclusionsFailedMeters, ServiceMetricNames.DAG_ACTION_CONCLUSIONS_FAILED);
    registerMetricForEachDagAction(this.dagActionConclusionsSucceededMeters, ServiceMetricNames.DAG_ACTION_CONCLUSIONS_SUCCEEDED);
  }

  /**
   * Create a meter of each dagActionType for the given metric, register it with the metric context, and store it in a
   * concurrent map.
   * @param metricMap
   * @param metricName
   */
  public void registerMetricForEachDagAction(ConcurrentMap<String, ContextAwareMeter> metricMap, String metricName) {
    for (DagActionStore.DagActionType dagActionType : DagActionStore.DagActionType.values()) {
      metricMap.put(dagActionType.toString(),
          this.metricContext.contextAwareMeter(metricName + dagActionType.toString()));
    }
  }
}
