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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
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
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.flow.FlowUtils;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.modules.utils.SharedFlowMetricsSingleton;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


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
  protected final TopologyCatalog topologyCatalog;
  protected final DagManager dagManager;

  protected final MetricContext metricContext;

  protected final EventSubmitter eventSubmitter;
  private final boolean isFlowConcurrencyEnabled;
  @Getter
  private Meter flowOrchestrationSuccessFulMeter;
  @Getter
  private Meter flowOrchestrationFailedMeter;
  @Getter
  private Timer flowOrchestrationTimer;
  private Counter flowFailedForwardToDagManagerCounter;
  @Setter
  private FlowStatusGenerator flowStatusGenerator;

  private UserQuotaManager quotaManager;
  private final FlowCompilationValidationHelper flowCompilationValidationHelper;
  private Optional<FlowTriggerHandler> flowTriggerHandler;
  private Optional<FlowCatalog> flowCatalog;
  @Getter
  private final SharedFlowMetricsSingleton sharedFlowMetricsSingleton;

  private final ClassAliasResolver<SpecCompiler> aliasResolver;

  @Inject
  public Orchestrator(Config config, TopologyCatalog topologyCatalog, DagManager dagManager,
      Optional<Logger> log, FlowStatusGenerator flowStatusGenerator, Optional<FlowTriggerHandler> flowTriggerHandler,
      SharedFlowMetricsSingleton sharedFlowMetricsSingleton, Optional<FlowCatalog> flowCatalog) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.aliasResolver = new ClassAliasResolver<>(SpecCompiler.class);
    this.topologyCatalog = topologyCatalog;
    this.dagManager = dagManager;
    this.flowStatusGenerator = flowStatusGenerator;
    this.flowTriggerHandler = flowTriggerHandler;
    this.sharedFlowMetricsSingleton = sharedFlowMetricsSingleton;
    this.flowCatalog = flowCatalog;
    try {
      String specCompilerClassName = ServiceConfigKeys.DEFAULT_GOBBLIN_SERVICE_FLOWCOMPILER_CLASS;
      if (config.hasPath(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY)) {
        specCompilerClassName = config.getString(ServiceConfigKeys.GOBBLIN_SERVICE_FLOWCOMPILER_CLASS_KEY);
      }
      _log.info("Using specCompiler class name/alias " + specCompilerClassName);

      this.specCompiler = (SpecCompiler) ConstructorUtils.invokeConstructor(Class.forName(this.aliasResolver.resolve(specCompilerClassName)), config);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException |
             ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    //At this point, the TopologySpecMap is initialized by the SpecCompiler. Pass the TopologySpecMap to the DagManager.
    this.dagManager.setTopologySpecMap(getSpecCompiler().getTopologySpecMap());

    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), this.specCompiler.getClass());
    this.flowOrchestrationSuccessFulMeter = this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_SUCCESSFUL_METER);
    this.flowOrchestrationFailedMeter = this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_FAILED_METER);
    this.flowOrchestrationTimer = this.metricContext.timer(ServiceMetricNames.FLOW_ORCHESTRATION_TIMER);
    this.flowFailedForwardToDagManagerCounter = this.metricContext.counter(ServiceMetricNames.FLOW_FAILED_FORWARD_TO_DAG_MANAGER_COUNT);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build();

    this.isFlowConcurrencyEnabled = ConfigUtils.getBoolean(config, ServiceConfigKeys.FLOW_CONCURRENCY_ALLOWED,
        ServiceConfigKeys.DEFAULT_FLOW_CONCURRENCY_ALLOWED);
    quotaManager = GobblinConstructorUtils.invokeConstructor(UserQuotaManager.class,
        ConfigUtils.getString(config, ServiceConfigKeys.QUOTA_MANAGER_CLASS, ServiceConfigKeys.DEFAULT_QUOTA_MANAGER),
        config);
    this.flowCompilationValidationHelper = new FlowCompilationValidationHelper(sharedFlowMetricsSingleton, specCompiler,
        quotaManager, eventSubmitter, flowStatusGenerator, isFlowConcurrencyEnabled);
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

    this.specCompiler.onDeleteSpec(deletedSpecURI, deletedSpecVersion, headers);
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

  public void orchestrate(Spec spec, Properties jobProps, long triggerTimestampMillis, boolean isReminderEvent)
      throws Exception {
    // Add below waiting because TopologyCatalog and FlowCatalog service can be launched at the same time
    this.topologyCatalog.getInitComplete().await();

    //Wait for the SpecCompiler to become healthy.
    this.getSpecCompiler().awaitHealthy();

    long startTime = System.nanoTime();
    if (spec instanceof FlowSpec) {
      FlowSpec flowSpec = (FlowSpec) spec;
      Config flowConfig = (flowSpec).getConfig();
      String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);

      sharedFlowMetricsSingleton.addFlowGauge(spec, flowConfig, flowGroup, flowName);

      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(flowSpec);
      String flowExecutionId = String.valueOf(FlowUtils.getOrCreateFlowExecutionId(flowSpec));

      DagActionStore.DagAction flowAction =
          new DagActionStore.DagAction(flowGroup, flowName, flowExecutionId, DagActionStore.FlowActionType.LAUNCH);

      // If multi-active scheduler is enabled do not pass onto DagManager, otherwise scheduler forwards it directly
      // Skip flow compilation as well, since we recompile after receiving event from DagActionStoreChangeMonitor later
      if (flowTriggerHandler.isPresent()) {

        // Adopt consensus flowExecutionId for scheduled flows
        flowTriggerHandler.get().handleTriggerEvent(jobProps, flowAction, triggerTimestampMillis, isReminderEvent,
            flowSpec.isScheduled());
        _log.info("Multi-active scheduler finished handling trigger event: [{}, is: {}, triggerEventTimestamp: {}]",
            flowAction, isReminderEvent ? "reminder" : "original", triggerTimestampMillis);
      } else {
        TimingEvent flowCompilationTimer = new TimingEvent(this.eventSubmitter, TimingEvent.FlowTimings.FLOW_COMPILED);
        Optional<Dag<JobExecutionPlan>> compiledDagOptional =
            this.flowCompilationValidationHelper.validateAndHandleConcurrentExecution(flowConfig, flowSpec, flowGroup,
                flowName, flowMetadata);

        if (!compiledDagOptional.isPresent()) {
          Instrumented.markMeter(this.flowOrchestrationFailedMeter);
          return;
        }
        Dag<JobExecutionPlan> compiledDag = compiledDagOptional.get();
        if (compiledDag.isEmpty()) {
          FlowCompilationValidationHelper.populateFlowCompilationFailedEventMessage(eventSubmitter, flowSpec, flowMetadata);
          Instrumented.markMeter(this.flowOrchestrationFailedMeter);
          sharedFlowMetricsSingleton.conditionallyUpdateFlowGaugeSpecState(spec,
              SharedFlowMetricsSingleton.CompiledState.FAILED);
          _log.warn("Cannot determine an executor to run on for Spec: " + spec);
          return;
        }
        sharedFlowMetricsSingleton.conditionallyUpdateFlowGaugeSpecState(spec,
            SharedFlowMetricsSingleton.CompiledState.SUCCESSFUL);

        FlowCompilationValidationHelper.addFlowExecutionIdIfAbsent(flowMetadata, compiledDag);
        flowCompilationTimer.stop(flowMetadata);

        // Depending on if DagManager is present, handle execution
        submitFlowToDagManager(flowSpec, compiledDag);
      }
    } else {
      Instrumented.markMeter(this.flowOrchestrationFailedMeter);
      throw new RuntimeException("Spec not of type FlowSpec, cannot orchestrate: " + spec);
    }
    Instrumented.markMeter(this.flowOrchestrationSuccessFulMeter);
    Instrumented.updateTimer(this.flowOrchestrationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
  }

  public void submitFlowToDagManager(FlowSpec flowSpec) throws IOException, InterruptedException {
    Optional<Dag<JobExecutionPlan>> optionalJobExecutionPlanDag =
        this.flowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec);
    if (optionalJobExecutionPlanDag.isPresent()) {
      submitFlowToDagManager(flowSpec, optionalJobExecutionPlanDag.get());
    } else {
      _log.warn("Flow: {} submitted to dagManager failed to compile and produce a job execution plan dag", flowSpec);
      Instrumented.markMeter(this.flowOrchestrationFailedMeter);
    }
  }

  public void submitFlowToDagManager(FlowSpec flowSpec, Dag<JobExecutionPlan> jobExecutionPlanDag)
      throws IOException {
    try {
      /* Send the dag to the DagManager
      Note that the responsibility of the multi-active scheduler mode ends after this method is completed AND the
      consumption of a launch type event is committed to the consumer.
       */
      this.dagManager.addDagAndRemoveAdhocFlowSpec(flowSpec, jobExecutionPlanDag, true, true);
    } catch (Exception ex) {
      String failureMessage = "Failed to add Job Execution Plan due to: " + ex.getMessage();
      _log.warn("Orchestrator call - " + failureMessage, ex);
      this.flowFailedForwardToDagManagerCounter.inc();
      // pronounce failed before stack unwinds, to ensure flow not marooned in `COMPILED` state; (failure likely attributable to DB connection/failover)
      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata(flowSpec);
      flowMetadata.put(TimingEvent.METADATA_MESSAGE, failureMessage);
      new TimingEvent(this.eventSubmitter, TimingEvent.FlowTimings.FLOW_FAILED).stop(flowMetadata);
      throw ex;
    }
  }

  public void remove(Spec spec, Properties headers) throws IOException {
    // TODO: Evolve logic to cache and reuse previously compiled JobSpecs
    // .. this will work for Identity compiler but not always for multi-hop.
    // Note: Current logic assumes compilation is consistent between all executions
    if (spec instanceof FlowSpec) {
      //Send the dag to the DagManager to stop it.
      //Also send it to the SpecProducer to do any cleanup tasks on SpecExecutor.
      _log.info("Forwarding cancel request for flow URI {} to DagManager.", spec.getUri());
      this.dagManager.stopDag(spec.getUri());
      // We need to recompile the flow to find the spec producer,
      // If compilation result is different, its remove request can go to some different spec producer
      deleteFromExecutor(spec, headers);
    } else {
      throw new RuntimeException("Spec not of type FlowSpec, cannot delete: " + spec);
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
}