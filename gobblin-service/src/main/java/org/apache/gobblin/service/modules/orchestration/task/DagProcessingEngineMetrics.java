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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.MetricTagNames;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.metrics.ServiceMetricNames;

/**
 * Used to track all metrics relating to processing dagActions when DagProcessingEngine is enabled. The metrics can be
 * used to trace the number of dagActions (which can be further broken down by type) at various points of the system,
 * starting from addition to the DagActionStore to observation in the DagActionChangeMonitor through the
 * DagProcessingEngine pipeline (DagManagement -> DagTaskStreamImpl -> MySqlMultiActiveLeaseArbiter -> DagProc).
 */
@Slf4j
public class DagProcessingEngineMetrics {
  MetricContext metricContext;
  /*
   Declare map of dagActionType to a ContextAwareMeter for each metric. ContextAwareMeters are thread safe, so it will
   handle concurrent mark requests correctly. ConcurrentMap is not needed since no updates are made to the mappings,
   only get calls.
  */
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsStoredMeterByDagActionType = new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsObservedMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsLeasesObtainedMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsNoLongerLeasingMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsLeaseReminderScheduledMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsReminderProcessedMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsExceededMaxRetryMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsInitializeFailedMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsInitializeSucceededMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsActFailedMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsActSucceededMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsConcludeFailedMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsConcludeSucceededMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsRemovedFromStoreMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionsFailingRemovalMeterByDagActionType =  new HashMap();
  private HashMap<DagActionStore.DagActionType, ContextAwareMeter> dagActionAverageProcessingDelayMillisMeterByDagActionType =  new HashMap();

  public DagProcessingEngineMetrics(MetricContext metricContext) {
    this.metricContext = metricContext;
    registerAllMetrics();
  }

  @Inject
  public DagProcessingEngineMetrics() {
    // Create a new metric context for the DagProcessingEngineMetrics tagged appropriately
    List<Tag<?>> tags = new ArrayList<>();
    tags.add(new Tag<>(MetricTagNames.METRIC_BACKEND_REPRESENTATION, GobblinMetrics.MetricType.COUNTER));
    this.metricContext = Instrumented.getMetricContext(new State(), this.getClass(), tags);
  }

  public void registerAllMetrics() {
    registerMetricForEachDagActionType(this.dagActionsStoredMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_STORED);
    registerMetricForEachDagActionType(this.dagActionsObservedMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_OBSERVED);
    registerMetricForEachDagActionType(this.dagActionsLeasesObtainedMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_LEASES_OBTAINED);
    registerMetricForEachDagActionType(this.dagActionsNoLongerLeasingMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_NO_LONGER_LEASING);
    registerMetricForEachDagActionType(this.dagActionsLeaseReminderScheduledMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_LEASE_REMINDER_SCHEDULED);
    registerMetricForEachDagActionType(this.dagActionsReminderProcessedMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_REMINDER_PROCESSED);
    registerMetricForEachDagActionType(this.dagActionsExceededMaxRetryMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_EXCEEDED_MAX_RETRY);
    registerMetricForEachDagActionType(this.dagActionsInitializeFailedMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_INITIALIZE_FAILED);
    registerMetricForEachDagActionType(this.dagActionsInitializeSucceededMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_INITIALIZE_SUCCEEDED);
    registerMetricForEachDagActionType(this.dagActionsActFailedMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_ACT_FAILED);
    registerMetricForEachDagActionType(this.dagActionsActSucceededMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_ACT_SUCCEEDED);
    registerMetricForEachDagActionType(this.dagActionsConcludeFailedMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_CONCLUDE_FAILED);
    registerMetricForEachDagActionType(this.dagActionsConcludeSucceededMeterByDagActionType, ServiceMetricNames.DAG_ACTIONS_CONCLUDE_SUCCEEDED);
  }

  /**
   * Create a meter of each dagActionType for the given metric, register it with the metric context, and store it in a
   * concurrent map.
   * @param metricMap
   * @param metricName
   */
  private void registerMetricForEachDagActionType(HashMap<DagActionStore.DagActionType, ContextAwareMeter> metricMap, String metricName) {
    for (DagActionStore.DagActionType dagActionType : DagActionStore.DagActionType.values()) {
      metricMap.put(dagActionType, this.metricContext.contextAwareMeter(metricName + dagActionType));
    }
  }

  public void markDagActionsStored(DagActionStore.DagActionType dagActionType) {
    updateMetricForDagActionType(this.dagActionsStoredMeterByDagActionType, dagActionType);
  }

  public void markDagActionsObserved(DagActionStore.DagActionType dagActionType) {
    updateMetricForDagActionType(this.dagActionsObservedMeterByDagActionType, dagActionType);
  }

  public void markDagActionsLeasedObtained(DagActionStore.LeaseParams leaseParams) {
    updateMetricForDagActionType(this.dagActionsLeasesObtainedMeterByDagActionType,
        leaseParams.getDagAction().getDagActionType());
  }

  public void markDagActionsNoLongerLeasing(DagActionStore.LeaseParams leaseParams) {
    updateMetricForDagActionType(this.dagActionsNoLongerLeasingMeterByDagActionType,
        leaseParams.getDagAction().getDagActionType());
  }

  public void markDagActionsLeaseReminderScheduled(DagActionStore.LeaseParams leaseParams) {
    updateMetricForDagActionType(this.dagActionsLeaseReminderScheduledMeterByDagActionType,
        leaseParams.getDagAction().getDagActionType());
  }

  public void markDagActionsRemindersProcessed(DagActionStore.LeaseParams leaseParams) {
    updateMetricForDagActionType(this.dagActionsReminderProcessedMeterByDagActionType,
        leaseParams.getDagAction().getDagActionType());
  }

  // TODO: implement evaluating max retries later
  public void markDagActionsExceedingMaxRetry(DagActionStore.DagActionType dagActionType) {
    updateMetricForDagActionType(this.dagActionsExceededMaxRetryMeterByDagActionType, dagActionType);
  }

  public void markDagActionsInitialize(DagActionStore.DagActionType dagActionType, boolean succeeded) {
    if (succeeded) {
      updateMetricForDagActionType(this.dagActionsInitializeSucceededMeterByDagActionType, dagActionType);
    } else {
      updateMetricForDagActionType(this.dagActionsInitializeFailedMeterByDagActionType, dagActionType);
    }
  }

  public void markDagActionsAct(DagActionStore.DagActionType dagActionType, boolean succeeded) {
    if (succeeded) {
      updateMetricForDagActionType(this.dagActionsActSucceededMeterByDagActionType, dagActionType);
    } else {
      updateMetricForDagActionType(this.dagActionsActFailedMeterByDagActionType, dagActionType);
    }
  }

  public void markDagActionsConclude(DagActionStore.DagActionType dagActionType, boolean succeeded) {
    if (succeeded) {
      updateMetricForDagActionType(this.dagActionsConcludeSucceededMeterByDagActionType, dagActionType);
    } else {
      updateMetricForDagActionType(this.dagActionsConcludeFailedMeterByDagActionType, dagActionType);
    }
  }


  /**
   * Generic helper used to increment a metric corresponding to the dagActionType in the provided map. It assumes the
   * meter for each dagActionType can be identified by its name.
   */
  private void updateMetricForDagActionType(HashMap<DagActionStore.DagActionType, ContextAwareMeter> metricMap,
      DagActionStore.DagActionType dagActionType) {
      if (metricMap.containsKey(dagActionType)) {
        metricMap.get(dagActionType).mark();
      } else {
        throw new RuntimeException(String.format("No meter exists for dagActionType %s in metricsMap %s",
            dagActionType, metricMap));
      }
  }
}
