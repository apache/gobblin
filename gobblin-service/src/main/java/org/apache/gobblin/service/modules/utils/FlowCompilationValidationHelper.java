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
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
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


/**
 * Helper class with functionality meant to be re-used between the DagManager and Orchestrator when launching
 * executions of a flow spec. In the common case, the Orchestrator receives a flow to orchestrate, performs necessary
 * validations, and forwards the execution responsibility to the DagManager. The DagManager's responsibility is to
 * carry out any flow action requests. However, with launch executions now being stored in the DagActionStateStore, on
 * restart or leadership change the DagManager has to perform validations before executing any launch actions the
 * previous leader was unable to complete. Rather than duplicating the code or introducing a circular dependency between
 * the DagManager and Orchestrator, this class is utilized to store the common functionality. It is stateful,
 * requiring all stateful pieces to be passed as input from the caller upon instantiating the helper.
 * Note: We expect further refactoring to be done to the DagManager in later stage of multi-active development, so we do
 * not attempt major reorganization as abstractions may change.
 */
@Slf4j
@Data
public final class FlowCompilationValidationHelper {
  private final SharedFlowMetricsSingleton sharedFlowMetricsSingleton;
  private final SpecCompiler specCompiler;
  private final UserQuotaManager quotaManager;
  private final Optional<EventSubmitter> eventSubmitter;
  private final FlowStatusGenerator flowStatusGenerator;
  private final boolean isFlowConcurrencyEnabled;

  /**
   * For a given a flowSpec, verifies that an execution is allowed (in case there is an ongoing execution) and the
   * flowspec can be compiled. If the pre-conditions hold, then a JobExecutionPlan is constructed and returned to the
   * caller.
   * @return jobExecutionPlan dag if one can be constructed for the given flowSpec
   */
  public Optional<Dag<JobExecutionPlan>> createExecutionPlanIfValid(FlowSpec flowSpec)
      throws IOException, InterruptedException {
    Config flowConfig = flowSpec.getConfig();
    String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);

    //Wait for the SpecCompiler to become healthy.
    specCompiler.awaitHealthy();

    Optional<TimingEvent> flowCompilationTimer =
        this.eventSubmitter.transform(submitter -> new TimingEvent(submitter, TimingEvent.FlowTimings.FLOW_COMPILED));
    Optional<Dag<JobExecutionPlan>> jobExecutionPlanDagOptional =
        validateAndHandleConcurrentExecution(flowConfig, flowSpec, flowGroup, flowName);
    Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(flowSpec);

    if (!jobExecutionPlanDagOptional.isPresent()) {
      return Optional.absent();
    }

    if (jobExecutionPlanDagOptional.get() == null || jobExecutionPlanDagOptional.get().isEmpty()) {
      populateFlowCompilationFailedEventMessage(eventSubmitter, flowSpec, flowMetadata);
      return Optional.absent();
    }

    addFlowExecutionIdIfAbsent(flowMetadata, jobExecutionPlanDagOptional.get());
    if (flowCompilationTimer.isPresent()) {
      flowCompilationTimer.get().stop(flowMetadata);
    }
    return jobExecutionPlanDagOptional;
  }

  /**
   * Checks if flowSpec disallows concurrent executions, and if so then checks if another instance of the flow is
   * already running and emits a FLOW FAILED event. Otherwise, this check passes.
   * @return Optional<Dag<JobExecutionPlan>> if caller allowed to execute flow and compile spec, else absent Optional
   * @throws IOException
   */
  public Optional<Dag<JobExecutionPlan>> validateAndHandleConcurrentExecution(Config flowConfig, Spec spec,
      String flowGroup, String flowName) throws IOException {
    boolean allowConcurrentExecution = ConfigUtils.getBoolean(flowConfig,
        ConfigurationKeys.FLOW_ALLOW_CONCURRENT_EXECUTION, isFlowConcurrencyEnabled);

    Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(spec);

    if (isExecutionPermitted(flowStatusGenerator, flowName, flowGroup, allowConcurrentExecution)) {
      return Optional.of(jobExecutionPlanDag);
    } else {
      log.warn("Another instance of flowGroup: {}, flowName: {} running; Skipping flow execution since "
          + "concurrent executions are disabled for this flow.", flowGroup, flowName);
      sharedFlowMetricsSingleton.conditionallyUpdateFlowGaugeSpecState(spec,
          SharedFlowMetricsSingleton.CompiledState.SKIPPED);
      Instrumented.markMeter(sharedFlowMetricsSingleton.getSkippedFlowsMeter());
      if (!isScheduledFlow((FlowSpec) spec)) {
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
      return Optional.absent();
    }
  }

  /**
   * Check if a FlowSpec instance is allowed to run.
   *
   * @param flowName
   * @param flowGroup
   * @param allowConcurrentExecution
   * @return true if the {@link FlowSpec} allows concurrent executions or if no other instance of the flow is currently RUNNING.
   */
  private boolean isExecutionPermitted(FlowStatusGenerator flowStatusGenerator, String flowName, String flowGroup,
      boolean allowConcurrentExecution) {
    return allowConcurrentExecution || !flowStatusGenerator.isFlowRunning(flowName, flowGroup);
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
  public static void addFlowExecutionIdIfAbsent(Map<String,String> flowMetadata,
      Dag<JobExecutionPlan> jobExecutionPlanDag) {
    flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        jobExecutionPlanDag.getNodes().get(0).getValue().getJobSpec().getConfigAsProperties().getProperty(
            ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
  }

  /**
   * Return true if the spec contains a schedule, false otherwise.
   */
  public static boolean isScheduledFlow(FlowSpec spec) {
    return spec.getConfigAsProperties().containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY);
  }
}
