/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.service.modules.orchestration.task;

import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.metrics.ServiceMetricNames;

@Slf4j
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
  private void registerMetricForEachDagAction(ConcurrentMap<String, ContextAwareMeter> metricMap, String metricName) {
    for (DagActionStore.DagActionType dagActionType : DagActionStore.DagActionType.values()) {
      metricMap.put(dagActionType.toString(),
          this.metricContext.contextAwareMeter(metricName + dagActionType));
    }
  }

  /**
   * Updates the meter corresponding to the metricsName and dagActionType provided if they match an existing metric
   * @param metricName
   * @param dagActionType
   */
  public void updateMetricForDagAction(String metricName, DagActionStore.DagActionType dagActionType) {
    switch (metricName) {
      case ServiceMetricNames.DAG_ACTIONS_STORED:
        updateMetricForDagAction(this.dagActionsStoredMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTIONS_OBSERVED:
        updateMetricForDagAction(this.dagActionsObservedMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTIONS_LEASES_OBTAINED:
        updateMetricForDagAction(this.dagActionsLeasesObtainedMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTIONS_NO_LONGER_LEASING:
        updateMetricForDagAction(this.dagActionsNoLongerLeasingMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTION_LEASE_REMINDERS_SCHEDULED:
        updateMetricForDagAction(this.dagActionLeaseRemindersScheduledMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTION_REMINDERS_PROCESSED:
        updateMetricForDagAction(this.dagActionRemindersProcessedMeters, dagActionType);
        break;
      // TODO: implement evaluating max retries later
        case ServiceMetricNames.DAG_ACTIONS_EXCEEDED_MAX_RETRY:
        updateMetricForDagAction(this.dagActionsExceededMaxRetryMeters, dagActionType);
          break;
      case ServiceMetricNames.DAG_ACTIONS_INIT_FAILED:
        updateMetricForDagAction(this.dagActionsInitFailedMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTIONS_INIT_SUCCEEDED:
        updateMetricForDagAction(this.dagActionsInitSucceededMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTION_EXECUTIONS_FAILED:
        updateMetricForDagAction(this.dagActionExecutionsFailedMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTION_EXECUTIONS_SUCCEEDED:
        updateMetricForDagAction(this.dagActionExecutionsSucceededMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTION_CONCLUSIONS_FAILED:
        updateMetricForDagAction(this.dagActionConclusionsFailedMeters, dagActionType);
        break;
      case ServiceMetricNames.DAG_ACTION_CONCLUSIONS_SUCCEEDED:
        updateMetricForDagAction(this.dagActionConclusionsSucceededMeters, dagActionType);
        break;
      default:
        log.warn("Skipping marking metric because no meter found to match it. Metric name: {}", metricName);
        break;
    }
  }

  /**
   * Generic helper used to increment a metric corresponding to the dagActionType in the provided map. It assumes the
   * meter for each dagActionType can be identified by its name.
   */
  private void updateMetricForDagAction(ConcurrentMap<String, ContextAwareMeter> metricMap,
      DagActionStore.DagActionType dagActionType) {
      if (metricMap.containsKey(dagActionType.toString())) {
        metricMap.get(dagActionType.toString()).mark();
      } else {
        log.warn("Skipping metric. No meter exists for dagActionType {} in metricsMap {}");
      }
  }
}
