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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.TimingEventUtils;
import org.apache.gobblin.service.modules.orchestration.UserQuotaManager;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Helper class with functionality meant to be re-used between the LaunchDagProc and Orchestrator when launching
 * executions of a flow spec. In the common case, the Orchestrator receives a flow to orchestrate, performs necessary
 * validations, and creates {@link org.apache.gobblin.service.modules.orchestration.DagActionStore.DagAction}s which the
 * {@link org.apache.gobblin.service.modules.orchestration.DagProcessingEngine} is responsible for processing.
 * However, with launch executions now being stored in the DagActionStateStore, on restart, the LaunchDagProc has to
 * perform validations before executing any launch actions the previous LaunchDagProc was unable to complete.
 * Rather than duplicating the code or introducing a circular dependency between the LaunchDagProc and Orchestrator,
 * this class is utilized to store the common functionality. It is stateful, requiring all stateful pieces to be passed
 * as input from the caller upon instantiating the helper.
 */
@Slf4j
@Data
@Singleton
public class FlowCompilationValidationHelper {
  private final SharedFlowMetricsSingleton sharedFlowMetricsSingleton;
  @Getter
  private final SpecCompiler specCompiler;
  private final UserQuotaManager quotaManager;
  private final EventSubmitter eventSubmitter;
  private final FlowStatusGenerator flowStatusGenerator;
  private final boolean isFlowConcurrencyEnabled;

  @Inject
  public FlowCompilationValidationHelper(Config config, SharedFlowMetricsSingleton sharedFlowMetricsSingleton,
      UserQuotaManager userQuotaManager, FlowStatusGenerator flowStatusGenerator) {
    try {
      String specCompilerClassName = ConfigUtils.getString(config, ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY,
          ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS);
      this.specCompiler = (SpecCompiler) ConstructorUtils.invokeConstructor(Class.forName(
          new ClassAliasResolver<>(SpecCompiler.class).resolve(specCompilerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException |
             ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.sharedFlowMetricsSingleton = sharedFlowMetricsSingleton;
    this.quotaManager = userQuotaManager;
    MetricContext metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), this.specCompiler.getClass());
    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.service").build();
    this.flowStatusGenerator = flowStatusGenerator;
    this.isFlowConcurrencyEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.FLOW_CONCURRENCY_ALLOWED,
        ServiceConfigKeys.DEFAULT_FLOW_CONCURRENCY_ALLOWED);
  }

  /**
   * For a given a flowSpec, verifies that an execution is allowed (in case there is an ongoing execution) and the
   * flowspec can be compiled. If the pre-conditions hold, then a JobExecutionPlan is constructed and returned to the
   * caller.
   * @param flowSpec
   * @return jobExecutionPlan dag if one can be constructed for the given flowSpec
   */
  public Optional<Dag<JobExecutionPlan>> createExecutionPlanIfValid(FlowSpec flowSpec)
      throws IOException, InterruptedException {
    Config flowConfig = flowSpec.getConfig();
    String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
    String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);

    //Wait for the SpecCompiler to become healthy.
    specCompiler.awaitHealthy();

    TimingEvent flowCompilationTimer = new TimingEvent(this.eventSubmitter, TimingEvent.FlowTimings.FLOW_COMPILED);
    Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(flowSpec);

    try {
      Optional<Dag<JobExecutionPlan>> jobExecutionPlanDagOptional =
          validateAndHandleConcurrentExecution(flowConfig, flowSpec, flowGroup, flowName, flowMetadata);

      if (!jobExecutionPlanDagOptional.isPresent()) {
        return Optional.absent();
      }

      if (jobExecutionPlanDagOptional.get().isEmpty()) {
        populateFlowCompilationFailedEventMessage(eventSubmitter, flowSpec, flowMetadata);
        return Optional.absent();
      }

      flowCompilationTimer.stop(flowMetadata);
      return jobExecutionPlanDagOptional;
    } catch (IOException e) {
      log.error("Encountered exception when attempting to compile and perform checks for flowGroup: {} flowName: {}",
          flowGroup, flowName);
      throw e;
    }
  }

  /**
   * Checks if flowSpec disallows concurrent executions, and if so then checks if another instance of the flow is
   * already running and emits a FLOW FAILED event. Otherwise, this check passes.
   * @return Optional<Dag<JobExecutionPlan>> if caller allowed to execute flow and compile flowSpec, else Optional.absent()
   * @throws IOException
   */
  public Optional<Dag<JobExecutionPlan>> validateAndHandleConcurrentExecution(Config flowConfig, FlowSpec flowSpec,
      String flowGroup, String flowName, Map<String,String> flowMetadata) throws IOException {
    boolean allowConcurrentExecution = Boolean.parseBoolean(ConfigUtils.getString(flowConfig,
        ConfigurationKeys.FLOW_ALLOW_CONCURRENT_EXECUTION, String.valueOf(this.isFlowConcurrencyEnabled)));

    Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(flowSpec);

    if (jobExecutionPlanDag == null || jobExecutionPlanDag.isEmpty()) {
      // Send FLOW_FAILED event
      flowMetadata.put(TimingEvent.METADATA_MESSAGE, "Unable to compile flowSpec to produce non-empty "
          + "jobExecutionPlanDag.");
      new TimingEvent(eventSubmitter, TimingEvent.FlowTimings.FLOW_FAILED).stop(flowMetadata);
      return Optional.absent();
    }
    addFlowExecutionIdIfAbsent(flowMetadata, jobExecutionPlanDag);

    if (isExecutionPermitted(flowStatusGenerator, flowGroup, flowName, allowConcurrentExecution,
        Long.parseLong(flowMetadata.get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD)))) {
      return Optional.fromNullable(jobExecutionPlanDag);
    } else {
      log.warn("Another instance of flowGroup: {}, flowName: {} running; Skipping flow execution since "
          + "concurrent executions are disabled for this flow.", flowGroup, flowName);
      sharedFlowMetricsSingleton.conditionallyUpdateFlowGaugeSpecState(flowSpec,
          SharedFlowMetricsSingleton.CompiledState.SKIPPED);
      Instrumented.markMeter(sharedFlowMetricsSingleton.getSkippedFlowsMeter());
      if (!flowSpec.isScheduled()) {
        // For ad-hoc flow, we might already increase quota, we need to decrease here
        for (Dag.DagNode dagNode : jobExecutionPlanDag.getStartNodes()) {
          quotaManager.releaseQuota(dagNode);
        }
      }
      // Send FLOW_FAILED event
      flowMetadata.put(TimingEvent.METADATA_MESSAGE, "Flow failed because another instance is running and concurrent "
          + "executions are disabled. Set flow.allowConcurrentExecution to true in the flowSpec to change this behaviour.");
      new TimingEvent(eventSubmitter, TimingEvent.FlowTimings.FLOW_FAILED).stop(flowMetadata);
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
  private boolean isExecutionPermitted(FlowStatusGenerator flowStatusGenerator, String flowGroup, String flowName,
      boolean allowConcurrentExecution, long flowExecutionId) {
    return allowConcurrentExecution || !flowStatusGenerator.isFlowRunning(flowName, flowGroup, flowExecutionId);
  }

  /**
   * Abstraction used to populate the message of and emit a FlowCompileFailed event for the Orchestrator.
   * @param flowSpec
   * @param flowMetadata
   */
  public static void populateFlowCompilationFailedEventMessage(EventSubmitter eventSubmitter,
      FlowSpec flowSpec, Map<String, String> flowMetadata) {
    // For scheduled flows, we do not insert the flowExecutionId into the FlowSpec. As a result, if the flow
    // compilation fails (i.e. we are unable to find a path), the metadata will not have flowExecutionId.
    // In this case, the current time is used as the flow executionId.
    flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        Long.toString(System.currentTimeMillis()));

    String message = "Flow was not compiled successfully.";
    if (!flowSpec.getCompilationErrors().isEmpty()) {
      message = message + " Compilation errors encountered: " + flowSpec.getCompilationErrors();
    }
    flowMetadata.put(TimingEvent.METADATA_MESSAGE, message);

    new TimingEvent(eventSubmitter, TimingEvent.FlowTimings.FLOW_COMPILE_FAILED).stop(flowMetadata);
  }

  /**
   * If it is a scheduled flow run without multi-active scheduler configuration (where the FlowSpec does not have a
   * flowExecutionId) and the flow compilation is successful, retrieve flowExecutionId from the JobSpec.
   */
  public static void addFlowExecutionIdIfAbsent(Map<String,String> flowMetadata,
      Dag<JobExecutionPlan> jobExecutionPlanDag) {
    flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
        jobExecutionPlanDag.getNodes().get(0).getValue().getJobSpec().getConfigAsProperties().getProperty(
            ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
  }
}
