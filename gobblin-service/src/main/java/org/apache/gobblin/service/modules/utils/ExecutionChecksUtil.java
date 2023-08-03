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

package org.apache.gobblin.service.modules.utils;

import com.google.common.base.Optional;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Map;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.util.ConfigUtils;
import org.slf4j.Logger;


/**
 * Made to reuse checks between DagManager and Orchestrator
 * // TODO: may be refactored in later stage of multi-active development if we rethink DagManager functionality and
 * // abstractions
 */
public final class ExecutionChecksUtil {


  /**
   * For a given a flowSpec, verifies that an execution is allowed (in case there is an ongoing execution) and the
   * flowspec can be compiled. If the pre-conditions hold, then a JobExecutionPlan is constructed and returned to the
   * caller.
   * @return jobExecutionPlan dag if one can be constructed for the given flowSpec
   */
  public static Optional<Dag<JobExecutionPlan>> handleChecksBeforeExecution(
      SharedFlowMetricsContainer sharedFlowMetricsContainer, SpecCompiler specCompiler, UserQuotaManager quotaManager,
      Optional<EventSubmitter> eventSubmitter, FlowStatusGenerator flowStatusGenerator, Logger log,
      boolean flowConcurrencyFlag, FlowSpec flowSpec)
      throws IOException, InterruptedException {
    Config flowConfig = flowSpec.getConfig();
    String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);
    if (!isExecutionPermittedHandler(sharedFlowMetricsContainer, specCompiler, quotaManager, eventSubmitter,
        flowStatusGenerator, log, flowConcurrencyFlag, flowConfig, flowSpec, flowName, flowGroup)) {
      return Optional.absent();
    }

    //Wait for the SpecCompiler to become healthy.
    specCompiler.awaitHealthy();

    Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(flowSpec);
    Optional<TimingEvent> flowCompilationTimer =
        eventSubmitter.transform(submitter -> new TimingEvent(submitter, TimingEvent.FlowTimings.FLOW_COMPILED));
    Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(flowSpec);

    if (jobExecutionPlanDag == null || jobExecutionPlanDag.isEmpty()) {
      populateFlowCompilationFailedEventMessage(eventSubmitter, flowSpec, flowMetadata);
      return Optional.absent();
    }

    addFlowExecutionIdIfAbsent(flowMetadata, jobExecutionPlanDag);
    if (flowCompilationTimer.isPresent()) {
      flowCompilationTimer.get().stop(flowMetadata);
    }
    return Optional.of(jobExecutionPlanDag);
  }

  /**
   * Checks if flowSpec disallows concurrent executions, and if so then checks if another instance of the flow is
   * already running and emits a FLOW FAILED event. Otherwise, this check passes.
   * @return true if caller can proceed to execute flow, false otherwise
   * @throws IOException
   */
  public static boolean isExecutionPermittedHandler(SharedFlowMetricsContainer sharedFlowMetricsContainer,
      SpecCompiler specCompiler, UserQuotaManager quotaManager, Optional<EventSubmitter> eventSubmitter,
      FlowStatusGenerator flowStatusGenerator, Logger log, boolean flowConcurrencyFlag, Config flowConfig, Spec spec,
      String flowName, String flowGroup) throws IOException {
    boolean allowConcurrentExecution = ConfigUtils.getBoolean(flowConfig,
        ConfigurationKeys.FLOW_ALLOW_CONCURRENT_EXECUTION, flowConcurrencyFlag);

    Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(spec);

    if (!isExecutionPermitted(flowStatusGenerator, flowName, flowGroup, allowConcurrentExecution)) {
      log.warn("Another instance of flowGroup: {}, flowName: {} running; Skipping flow execution since "
          + "concurrent executions are disabled for this flow.", flowGroup, flowName);
      sharedFlowMetricsContainer.conditionallyUpdateFlowGaugeSpecState(spec, SharedFlowMetricsContainer.CompiledState.SKIPPED);
      Instrumented.markMeter(sharedFlowMetricsContainer.getSkippedFlowsMeter());
      if (!((FlowSpec) spec).getConfigAsProperties().containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        // For ad-hoc flow, we might already increase quota, we need to decrease here
        for (Dag.DagNode dagNode : jobExecutionPlanDag.getStartNodes()) {
          quotaManager.releaseQuota(dagNode);
        }
      }

      // Send FLOW_FAILED event
      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata((FlowSpec) spec);
      flowMetadata.put(TimingEvent.METADATA_MESSAGE, "Flow failed because another instance is running and concurrent "
          + "executions are disabled. Set flow.allowConcurrentExecution to true in the flow spec to change this behaviour.");
      if (eventSubmitter.isPresent()) {
        new TimingEvent(eventSubmitter.get(), TimingEvent.FlowTimings.FLOW_FAILED).stop(flowMetadata);
      }
      return false;
    }
    return true;
  }

  /**
   * Check if a FlowSpec instance is allowed to run.
   *
   * @param flowName
   * @param flowGroup
   * @param allowConcurrentExecution
   * @return true if the {@link FlowSpec} allows concurrent executions or if no other instance of the flow is currently RUNNING.
   */
  private static boolean isExecutionPermitted(FlowStatusGenerator flowStatusGenerator, String flowName,
      String flowGroup, boolean allowConcurrentExecution) {
    if (allowConcurrentExecution) {
      return true;
    } else {
      return !flowStatusGenerator.isFlowRunning(flowName, flowGroup);
    }
  }

  /**
   * Abstraction used to populate the message of and emit a FlowCompileFailed event for the Orchestrator.
   * @param spec
   * @param flowMetadata
   */
  public static void populateFlowCompilationFailedEventMessage(Optional<EventSubmitter> eventSubmitter, Spec spec,
      Map<String, String> flowMetadata) {
    // For scheduled flows, we do not insert the flowExecutionId into the FlowSpec. As a result, if the flow
    // compilation fails (i.e. we are unable to find a path), the metadata will not have flowExecutionId.
    // In this case, the current time is used as the flow executionId.
    flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        Long.toString(System.currentTimeMillis()));

    String message = "Flow was not compiled successfully.";
    if (!((FlowSpec) spec).getCompilationErrors().isEmpty()) {
      message = message + " Compilation errors encountered: " + ((FlowSpec) spec).getCompilationErrors();
    }
    flowMetadata.put(TimingEvent.METADATA_MESSAGE, message);

    Optional<TimingEvent> flowCompileFailedTimer = eventSubmitter.transform(submitter ->
        new TimingEvent(submitter, TimingEvent.FlowTimings.FLOW_COMPILE_FAILED));

    if (flowCompileFailedTimer.isPresent()) {
      flowCompileFailedTimer.get().stop(flowMetadata);
    }
  }

  /**
   * If it is a scheduled flow (and hence, does not have flowExecutionId in the FlowSpec) and the flow compilation is
   * successful, retrieve the flowExecutionId from the JobSpec.
   */
  public static void addFlowExecutionIdIfAbsent(Map<String,String> flowMetadata, Dag<JobExecutionPlan> jobExecutionPlanDag) {
    flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        jobExecutionPlanDag.getNodes().get(0).getValue().getJobSpec().getConfigAsProperties().getProperty(
            ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
  }
}
