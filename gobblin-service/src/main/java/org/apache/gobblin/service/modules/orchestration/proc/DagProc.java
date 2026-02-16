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

package org.apache.gobblin.service.modules.orchestration.proc;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.MDC;

import com.typesafe.config.Config;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.flowgraph.DagNodeId;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.orchestration.troubleshooter.ServiceLayerTroubleshooter;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Responsible for performing the actual work for a given {@link DagTask} by first initializing its state, performing
 * actions based on the type of {@link DagTask}. Submitting events in time is found to be important in
 * <a href="https://github.com/apache/gobblin/pull/3641">PR#3641</a>, hence initialize and act methods submit events as
 * they happen.
 * Parameter T is the type of object that needs an initialisation before the dag proc does the main work in `act` method.
 * This object is then passed to `act` method.
 *
 * @param <T> type of the initialization "state" on which to {@link DagProc#act}
 */
@Alpha
@Data
@Slf4j
public abstract class DagProc<T> {
  protected final DagTask dagTask;
  @Getter protected final Dag.DagId dagId;
  @Getter protected final DagNodeId dagNodeId;
  protected final Config config;
  protected static final MetricContext metricContext = Instrumented.getMetricContext(new State(), DagProc.class);
  protected static final EventSubmitter eventSubmitter = new EventSubmitter.Builder(
      metricContext, "org.apache.gobblin.service").build();

  /**
   * Flag to enable/disable service-layer troubleshooting.
   * Can be set via config: {@link ServiceConfigKeys#SERVICE_LAYER_TROUBLESHOOTER_ENABLED}
   */
  private final boolean serviceLayerTroubleshooterEnabled;

  public DagProc(DagTask dagTask, Config config) {
    this.dagTask = dagTask;
    this.config = config;
    this.dagId = DagUtils.generateDagId(this.dagTask.getDagAction().getFlowGroup(),
        this.dagTask.getDagAction().getFlowName(), this.dagTask.getDagAction().getFlowExecutionId());
    this.dagNodeId = this.dagTask.getDagAction().getDagNodeId();
    this.serviceLayerTroubleshooterEnabled = ConfigUtils.getBoolean(config,
        ServiceConfigKeys.SERVICE_LAYER_TROUBLESHOOTER_ENABLED,
        ServiceConfigKeys.DEFAULT_SERVICE_LAYER_TROUBLESHOOTER_ENABLED);
  }

  /**
   * Main processing method for DagProc that orchestrates the full lifecycle:
   * 1. Sets up MDC context for flow/job identification
   * 2. Starts ServiceLayerTroubleshooter (if enabled) to capture service-layer errors
   * 3. Initializes state
   * 4. Performs actions
   * 5. Reports captured issues as events
   * 6. Cleans up MDC context
   *
   * <p>MDC context is managed using try-with-resources to ensure automatic cleanup
   * even if exceptions occur. This prevents stale context from contaminating subsequent
   * DagProc executions on the same thread.
   *
   * @param dagManagementStateStore State store for DAG management operations
   * @param dagProcEngineMetrics Metrics for tracking DagProc execution
   * @throws IOException if processing fails
   */
  public final void process(DagManagementStateStore dagManagementStateStore,
      DagProcessingEngineMetrics dagProcEngineMetrics) throws IOException {

    // Extract flow/job identifiers for MDC context
    String flowGroup = this.dagId.getFlowGroup();
    String flowName = this.dagId.getFlowName();
    String flowExecutionId = String.valueOf(this.dagId.getFlowExecutionId());
    String jobName = this.dagNodeId != null ? this.dagNodeId.getJobName() : JobStatusRetriever.NA_KEY;

    // Set MDC context using try-with-resources for automatic cleanup
    // This ensures each DagProc execution has clean, isolated context
    try (
        Closeable c1 = MDC.putCloseable(ConfigurationKeys.FLOW_GROUP_KEY, flowGroup);
        Closeable c2 = MDC.putCloseable(ConfigurationKeys.FLOW_NAME_KEY, flowName);
        Closeable c3 = MDC.putCloseable(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, flowExecutionId);
        Closeable c4 = MDC.putCloseable(ConfigurationKeys.JOB_NAME_KEY, jobName)
    ) {

      // Start service-layer troubleshooter if enabled
      ServiceLayerTroubleshooter troubleshooter = null;
      if (serviceLayerTroubleshooterEnabled) {
        int maxIssuesPerExecution = ConfigUtils.getInt(
            this.config,
            ServiceConfigKeys.SERVICE_LAYER_TROUBLESHOOTER_MAX_ISSUES_PER_EXECUTION,
            ServiceConfigKeys.DEFAULT_SERVICE_LAYER_TROUBLESHOOTER_MAX_ISSUES_PER_EXECUTION);
        troubleshooter = new ServiceLayerTroubleshooter(true, maxIssuesPerExecution);
        troubleshooter.start();
        log.info("ServiceLayerTroubleshooter started for {}", contextualizeStatus(""));
      }

      T state = null;
      try {
        // Phase 1: Initialize
        logContextualizedInfo("initializing");
        state = initialize(dagManagementStateStore);
        dagProcEngineMetrics.markDagActionsInitialize(getDagActionType(), true);

        // Phase 2: Act
        logContextualizedInfo("ready to process");
        act(dagManagementStateStore, state, dagProcEngineMetrics);
        logContextualizedInfo("processed");

      } catch (Exception e) {
        // Mark appropriate phase as failed
        if (state == null) {
          dagProcEngineMetrics.markDagActionsInitialize(getDagActionType(), false);
        } else {
          dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
        }
        throw e;

      } finally {
        // Always finalize troubleshooting, even if exceptions occurred
        if (troubleshooter != null) {
          finalizeServiceLayerTroubleshooting(troubleshooter);
        }
      }

    } // MDC automatically cleared here via Closeable.close()
  }

  /**
   * Finalizes service-layer troubleshooting by stopping the appender,
   * logging issue summary, and submitting issues as events.
   *
   * <p>This method swallows exceptions to prevent troubleshooting failures
   * from masking the actual DagProc execution failure.
   *
   * @param troubleshooter The troubleshooter instance to finalize
   */
  private void finalizeServiceLayerTroubleshooting(ServiceLayerTroubleshooter troubleshooter) {
    try {
      troubleshooter.stop();
      troubleshooter.logIssueSummary();
      troubleshooter.reportIssuesAsEvents(eventSubmitter);
      log.debug("ServiceLayerTroubleshooter finalized for {}. Captured {} issues.",
          contextualizeStatus(""), troubleshooter.getIssueCount());
    } catch (Exception e) {
      log.error("Failed to finalize service-layer troubleshooting for {}. " +
          "Issues may not be reported.", contextualizeStatus(""), e);
    }
  }

  protected abstract T initialize(DagManagementStateStore dagManagementStateStore) throws IOException;

  protected abstract void act(DagManagementStateStore dagManagementStateStore, T state,
      DagProcessingEngineMetrics dagProcEngineMetrics) throws IOException;

  public DagActionStore.DagActionType getDagActionType() {
    return this.dagTask.getDagAction().getDagActionType();
  }

  /** @return `message` formatted with class name and {@link #getDagId()} */
  public String contextualizeStatus(String message) {
    return String.format("%s - %s - dagId : %s", getClass().getSimpleName(), message, this.dagId);
  }

  /** INFO-level logging for `message` with class name and {@link #getDagId()} */
  public void logContextualizedInfo(String message) {
    log.info(contextualizeStatus(message));
  }
}
