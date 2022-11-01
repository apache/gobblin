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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareGauge;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;


/**
 * Orchestrator that is a {@link SpecCatalogListener}. It listens to changes
 * to {@link TopologyCatalog} and updates {@link SpecCompiler} state
 * Also it listens to {@link org.apache.gobblin.runtime.spec_catalog.FlowCatalog} and use the compiler to compile the new flow spec.
 */
@Alpha
@Singleton
public class Orchestrator implements SpecCatalogListener, Instrumentable {

  protected final Logger _log;
  protected final SpecCompiler specCompiler;
  protected final Optional<TopologyCatalog> topologyCatalog;
  protected final Optional<DagManager> dagManager;

  protected final MetricContext metricContext;

  protected final Optional<EventSubmitter> eventSubmitter;
  private final boolean flowConcurrencyFlag;
  @Getter
  private Optional<Meter> flowOrchestrationSuccessFulMeter;
  @Getter
  private Optional<Meter> flowOrchestrationFailedMeter;
  @Getter
  private Optional<Timer> flowOrchestrationTimer;
  private Optional<Meter> skippedFlowsMeter;
  @Setter
  private FlowStatusGenerator flowStatusGenerator;

  private final ClassAliasResolver<SpecCompiler> aliasResolver;

  private Map<String, FlowCompiledState> flowGauges = Maps.newHashMap();


  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog, Optional<DagManager> dagManager, Optional<Logger> log,
      FlowStatusGenerator flowStatusGenerator, boolean instrumentationEnabled) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.aliasResolver = new ClassAliasResolver<>(SpecCompiler.class);
    this.topologyCatalog = topologyCatalog;
    this.dagManager = dagManager;
    this.flowStatusGenerator = flowStatusGenerator;

    try {
      String specCompilerClassName = ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS;
      if (config.hasPath(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY)) {
        specCompilerClassName = config.getString(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY);
      }
      _log.info("Using specCompiler class name/alias " + specCompilerClassName);

      this.specCompiler = (SpecCompiler) ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(
          specCompilerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    //At this point, the TopologySpecMap is initialized by the SpecCompiler. Pass the TopologySpecMap to the DagManager.
    if (this.dagManager.isPresent()) {
      this.dagManager.get().setTopologySpecMap(getSpecCompiler().getTopologySpecMap());
    }

    if (instrumentationEnabled) {
      this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), this.specCompiler.getClass());
      this.flowOrchestrationSuccessFulMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_SUCCESSFUL_METER));
      this.flowOrchestrationFailedMeter = Optional.of(this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_FAILED_METER));
      this.flowOrchestrationTimer = Optional.of(this.metricContext.timer(ServiceMetricNames.FLOW_ORCHESTRATION_TIMER));
      this.skippedFlowsMeter = Optional.of(metricContext.contextAwareMeter(ServiceMetricNames.SKIPPED_FLOWS));
      this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
    } else {
      this.metricContext = null;
      this.flowOrchestrationSuccessFulMeter = Optional.absent();
      this.flowOrchestrationFailedMeter = Optional.absent();
      this.flowOrchestrationTimer = Optional.absent();
      this.skippedFlowsMeter = Optional.absent();
      this.eventSubmitter = Optional.absent();
    }
    this.flowConcurrencyFlag = ConfigUtils.getBoolean(config, ServiceConfigKeys.FLOW_CONCURRENCY_ALLOWED,
        ServiceConfigKeys.DEFAULT_FLOW_CONCURRENCY_ALLOWED);
  }

  @Inject
  public Orchestrator(Config config, FlowStatusGenerator flowStatusGenerator, Optional<TopologyCatalog> topologyCatalog,
      Optional<DagManager> dagManager, Optional<Logger> log) {
    this(config, topologyCatalog, dagManager, log, flowStatusGenerator, true);
  }


  @VisibleForTesting
  public SpecCompiler getSpecCompiler() {
    return this.specCompiler;
  }

  /** {@inheritDoc} */
  @Override
  public AddSpecResponse onAddSpec(Spec addedSpec) {
    if (addedSpec instanceof TopologySpec) {
      _log.info("New Spec detected of type TopologySpec: " + addedSpec);
      this.specCompiler.onAddSpec(addedSpec);
    } else if (addedSpec instanceof FlowSpec) {
      _log.info("New Spec detected of type FlowSpec: " + addedSpec);
      return this.specCompiler.onAddSpec(addedSpec);
    }
    return new AddSpecResponse(null);
  }

  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion) {
    onDeleteSpec(deletedSpecURI, deletedSpecVersion, new Properties());
  }

  /** {@inheritDoc} */
  @Override
  public void onDeleteSpec(URI deletedSpecURI, String deletedSpecVersion, Properties headers) {
    _log.info("Spec deletion detected: " + deletedSpecURI + "/" + deletedSpecVersion);

    if (topologyCatalog.isPresent()) {
      this.specCompiler.onDeleteSpec(deletedSpecURI, deletedSpecVersion, headers);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void onUpdateSpec(Spec updatedSpec) {
    _log.info("Spec changed: " + updatedSpec);
    if (updatedSpec instanceof FlowSpec) {
      onAddSpec(updatedSpec);
    }

    if (!(updatedSpec instanceof TopologySpec)) {
      return;
    }

    try {
      onDeleteSpec(updatedSpec.getUri(), updatedSpec.getVersion());
    } catch (Exception e) {
      _log.error("Failed to update Spec: " + updatedSpec, e);
    }
    try {
      onAddSpec(updatedSpec);
    } catch (Exception e) {
      _log.error("Failed to update Spec: " + updatedSpec, e);
    }

  }

  public void orchestrate(Spec spec) throws Exception {
    // Add below waiting because TopologyCatalog and FlowCatalog service can be launched at the same time
    this.topologyCatalog.get().getInitComplete().await();

    //Wait for the SpecCompiler to become healthy.
    this.getSpecCompiler().awaitHealthy();

    long startTime = System.nanoTime();
    if (spec instanceof FlowSpec) {
      Config flowConfig = ((FlowSpec) spec).getConfig();
      String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);

      // Only register the metric of flows that are scheduled, run once flows should not be tracked indefinitely
      if (!flowGauges.containsKey(spec.getUri().toString()) && flowConfig.hasPath(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        String flowCompiledGaugeName = MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX, flowGroup, flowName, ServiceMetricNames.COMPILED);
        flowGauges.put(spec.getUri().toString(), new FlowCompiledState());
        ContextAwareGauge<Integer> gauge = RootMetricContext.get().newContextAwareGauge(flowCompiledGaugeName, () -> flowGauges.get(spec.getUri().toString()).state.value);
        RootMetricContext.get().register(flowCompiledGaugeName, gauge);
      }

      //If the FlowSpec disallows concurrent executions, then check if another instance of the flow is already
      //running. If so, return immediately.
      boolean allowConcurrentExecution = ConfigUtils
          .getBoolean(flowConfig, ConfigurationKeys.FLOW_ALLOW_CONCURRENT_EXECUTION, this.flowConcurrencyFlag);

      if (!canRun(flowName, flowGroup, allowConcurrentExecution)) {
        _log.warn("Another instance of flowGroup: {}, flowName: {} running; Skipping flow execution since "
            + "concurrent executions are disabled for this flow.", flowGroup, flowName);
        conditionallyUpdateFlowGaugeSpecState(spec, CompiledState.SKIPPED);
        Instrumented.markMeter(this.skippedFlowsMeter);

        // Send FLOW_FAILED event
        Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata((FlowSpec) spec);
        flowMetadata.put(TimingEvent.METADATA_MESSAGE, "Flow failed because another instance is running and concurrent "
            + "executions are disabled. Set flow.allowConcurrentExecution to true in the flow spec to change this behaviour.");
        if (this.eventSubmitter.isPresent()) {
          new TimingEvent(this.eventSubmitter.get(), TimingEvent.FlowTimings.FLOW_FAILED).stop(flowMetadata);
        }
        return;
      }

      Optional<TimingEvent> flowCompilationTimer = this.eventSubmitter.transform(submitter ->
          new TimingEvent(submitter, TimingEvent.FlowTimings.FLOW_COMPILED));

      Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(spec);

      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata((FlowSpec) spec);
      if (jobExecutionPlanDag == null || jobExecutionPlanDag.isEmpty()) {
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

        Optional<TimingEvent> flowCompileFailedTimer = this.eventSubmitter.transform(submitter ->
            new TimingEvent(submitter, TimingEvent.FlowTimings.FLOW_COMPILE_FAILED));
        Instrumented.markMeter(this.flowOrchestrationFailedMeter);
        conditionallyUpdateFlowGaugeSpecState(spec, CompiledState.FAILED);
        _log.warn("Cannot determine an executor to run on for Spec: " + spec);
        if (flowCompileFailedTimer.isPresent()) {
          flowCompileFailedTimer.get().stop(flowMetadata);
        }
        return;
      }
      conditionallyUpdateFlowGaugeSpecState(spec, CompiledState.SUCCESSFUL);

      //If it is a scheduled flow (and hence, does not have flowExecutionId in the FlowSpec) and the flow compilation is successful,
      // retrieve the flowExecutionId from the JobSpec.
      flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
          jobExecutionPlanDag.getNodes().get(0).getValue().getJobSpec().getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));

      if (flowCompilationTimer.isPresent()) {
        flowCompilationTimer.get().stop(flowMetadata);
      }

      if (this.dagManager.isPresent()) {
        try {
          //Send the dag to the DagManager.
          this.dagManager.get().addDag(jobExecutionPlanDag, true, true);
        } catch (Exception ex) {
          if (this.eventSubmitter.isPresent()) {
            // pronounce failed before stack unwinds, to ensure flow not marooned in `COMPILED` state; (failure likely attributable to DB connection/failover)
            String failureMessage = "Failed to add Job Execution Plan due to: " + ex.getMessage();
            flowMetadata.put(TimingEvent.METADATA_MESSAGE, failureMessage);
            new TimingEvent(this.eventSubmitter.get(), TimingEvent.FlowTimings.FLOW_FAILED).stop(flowMetadata);
          }
          throw ex;
        }
      } else {
        // Schedule all compiled JobSpecs on their respective Executor
        for (Dag.DagNode<JobExecutionPlan> dagNode : jobExecutionPlanDag.getNodes()) {
          DagManagerUtils.incrementJobAttempt(dagNode);
          JobExecutionPlan jobExecutionPlan = dagNode.getValue();

          // Run this spec on selected executor
          SpecProducer producer = null;
          try {
            producer = jobExecutionPlan.getSpecExecutor().getProducer().get();
            Spec jobSpec = jobExecutionPlan.getJobSpec();

            if (!((JobSpec) jobSpec).getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
              _log.warn("JobSpec does not contain flowExecutionId.");
            }

            Map<String, String> jobMetadata = TimingEventUtils.getJobMetadata(flowMetadata, jobExecutionPlan);
            _log.info(String.format("Going to orchestrate JobSpec: %s on Executor: %s", jobSpec, producer));

            Optional<TimingEvent> jobOrchestrationTimer = this.eventSubmitter.transform(submitter ->
                new TimingEvent(submitter, TimingEvent.LauncherTimings.JOB_ORCHESTRATED));

            producer.addSpec(jobSpec);

            if (jobOrchestrationTimer.isPresent()) {
              jobOrchestrationTimer.get().stop(jobMetadata);
            }
          } catch (Exception e) {
            _log.error("Cannot successfully setup spec: " + jobExecutionPlan.getJobSpec() + " on executor: " + producer
                + " for flow: " + spec, e);
          }
        }
      }
    } else {
      Instrumented.markMeter(this.flowOrchestrationFailedMeter);
      throw new RuntimeException("Spec not of type FlowSpec, cannot orchestrate: " + spec);
    }
    Instrumented.markMeter(this.flowOrchestrationSuccessFulMeter);
    Instrumented.updateTimer(this.flowOrchestrationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
  }

  /**
   * Check if a FlowSpec instance is allowed to run.
   *
   * @param flowName
   * @param flowGroup
   * @param allowConcurrentExecution
   * @return true if the {@link FlowSpec} allows concurrent executions or if no other instance of the flow is currently RUNNING.
   */
  private boolean canRun(String flowName, String flowGroup, boolean allowConcurrentExecution) {
    if (allowConcurrentExecution) {
      return true;
    } else {
      return !flowStatusGenerator.isFlowRunning(flowName, flowGroup);
    }
  }

  public void remove(Spec spec, Properties headers) throws IOException {
    // TODO: Evolve logic to cache and reuse previously compiled JobSpecs
    // .. this will work for Identity compiler but not always for multi-hop.
    // Note: Current logic assumes compilation is consistent between all executions
    if (spec instanceof FlowSpec) {
      //Send the dag to the DagManager to stop it.
      //Also send it to the SpecProducer to do any cleanup tasks on SpecExecutor.
      if (this.dagManager.isPresent()) {
        _log.info("Forwarding cancel request for flow URI {} to DagManager.", spec.getUri());
        this.dagManager.get().stopDag(spec.getUri());
      }
      // We need to recompile the flow to find the spec producer,
      // If compilation result is different, its remove request can go to some different spec producer
      deleteFromExecutor(spec, headers);
    } else {
      throw new RuntimeException("Spec not of type FlowSpec, cannot delete: " + spec);
    }
  }

  private void deleteFromExecutor(Spec spec, Properties headers) {
    Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(spec);

    if (jobExecutionPlanDag.isEmpty()) {
      _log.warn("Cannot determine an executor to delete Spec: " + spec);
      return;
    }

    // Delete all compiled JobSpecs on their respective Executor
    for (Dag.DagNode<JobExecutionPlan> dagNode : jobExecutionPlanDag.getNodes()) {
      JobExecutionPlan jobExecutionPlan = dagNode.getValue();
      JobSpec jobSpec = jobExecutionPlan.getJobSpec();
      try {
        SpecProducer<Spec> producer = jobExecutionPlan.getSpecExecutor().getProducer().get();
        if (jobSpec.getConfig().hasPath(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
          headers.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, jobSpec.getConfig().getString(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));
        }
        _log.info(String.format("Going to delete JobSpec: %s on Executor: %s", jobSpec, producer));
        producer.deleteSpec(jobSpec.getUri(), headers);
      } catch (Exception e) {
        _log.error(String.format("Could not delete JobSpec: %s for flow: %s", jobSpec, spec), e);
      }
    }
  }

  /**
   * Updates the flowgauge related to the spec if the gauge is being tracked for the flow
   * @param spec FlowSpec to be updated
   * @param state desired state to set the gauge
   */
  private void conditionallyUpdateFlowGaugeSpecState(Spec spec, CompiledState state) {
    if (this.flowGauges.containsKey(spec.getUri().toString())) {
      this.flowGauges.get(spec.getUri().toString()).setState(state);
    }
  }


  @Nonnull
  @Override
  public MetricContext getMetricContext() {
    return this.metricContext;
  }

  @Override
  public boolean isInstrumentationEnabled() {
    return null != this.metricContext;
  }

  @Override
  public List<Tag<?>> generateTags(State state) {
    return Collections.emptyList();
  }

  @Override
  public void switchMetricContext(List<Tag<?>> tags) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void switchMetricContext(MetricContext context) {
    throw new UnsupportedOperationException();
  }

  @Setter
  private static class FlowCompiledState {
    private CompiledState state = CompiledState.UNKNOWN;
  }

  private enum CompiledState {
    FAILED(-1),
    UNKNOWN(0),
    SUCCESSFUL(1),
    SKIPPED(2);

    public int value;

    CompiledState(int value) {
      this.value = value;
    }
  }
}