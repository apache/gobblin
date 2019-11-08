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
import org.apache.gobblin.metrics.reporter.util.MetricReportUtils;
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
import org.apache.gobblin.service.monitoring.FsJobStatusRetriever;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
import org.apache.gobblin.util.ClassAliasResolver;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


/**
 * Orchestrator that is a {@link SpecCatalogListener}. It listens to changes
 * to {@link TopologyCatalog} and updates {@link SpecCompiler} state.
 */
@Alpha
public class Orchestrator implements SpecCatalogListener, Instrumentable {
  private static final String JOB_STATUS_RETRIEVER_CLASS_KEY = "jobStatusRetriever.class";

  protected final Logger _log;
  protected final SpecCompiler specCompiler;
  protected final Optional<TopologyCatalog> topologyCatalog;
  protected final Optional<DagManager> dagManager;

  protected final MetricContext metricContext;

  protected final Optional<EventSubmitter> eventSubmitter;
  @Getter
  private Optional<Meter> flowOrchestrationSuccessFulMeter;
  @Getter
  private Optional<Meter> flowOrchestrationFailedMeter;
  @Getter
  private Optional<Timer> flowOrchestrationTimer;
  @Setter
  private FlowStatusGenerator flowStatusGenerator;

  private final ClassAliasResolver<SpecCompiler> aliasResolver;

  private Map<String, FlowCompiledState> flowGauges = Maps.newHashMap();

  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog, Optional<DagManager> dagManager, Optional<Logger> log,
      boolean instrumentationEnabled) {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.aliasResolver = new ClassAliasResolver<>(SpecCompiler.class);
    this.topologyCatalog = topologyCatalog;
    this.dagManager = dagManager;
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
      this.eventSubmitter = Optional.of(new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build());
    } else {
      this.metricContext = null;
      this.flowOrchestrationSuccessFulMeter = Optional.absent();
      this.flowOrchestrationFailedMeter = Optional.absent();
      this.flowOrchestrationTimer = Optional.absent();
      this.eventSubmitter = Optional.absent();
    }
  }

  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog, Optional<DagManager> dagManager, Optional<Logger> log) {
    this(config, topologyCatalog, dagManager, log, true);
  }

  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog, Optional<DagManager> dagManager, Logger log) {
    this(config, topologyCatalog, dagManager, Optional.of(log));
  }

  public Orchestrator(Config config, Logger log) {
    this(config, Optional.<TopologyCatalog>absent(), Optional.<DagManager>absent(), Optional.of(log));
  }

  /** Constructor with no logging */
  public Orchestrator(Config config, Optional<TopologyCatalog> topologyCatalog) {
    this(config, topologyCatalog, Optional.<DagManager>absent(), Optional.<Logger>absent());
  }

  public Orchestrator(Config config) {
    this(config, Optional.<TopologyCatalog>absent(), Optional.<DagManager>absent(), Optional.<Logger>absent());
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
      TimingEvent flowCompilationTimer = this.eventSubmitter.isPresent()
          ? this.eventSubmitter.get().getTimingEvent(TimingEvent.FlowTimings.FLOW_COMPILED)
          : null;

      Config flowConfig = ((FlowSpec) spec).getConfig();
      String flowGroup = flowConfig.getString(ConfigurationKeys.FLOW_GROUP_KEY);
      String flowName = flowConfig.getString(ConfigurationKeys.FLOW_NAME_KEY);

      if (!flowGauges.containsKey(spec.getUri().toString())) {
        String flowCompiledGaugeName = MetricRegistry.name(MetricReportUtils.GOBBLIN_SERVICE_METRICS_PREFIX, flowGroup, flowName, ServiceMetricNames.COMPILED);
        flowGauges.put(spec.getUri().toString(), new FlowCompiledState());
        ContextAwareGauge<Integer> gauge = RootMetricContext.get().newContextAwareGauge(flowCompiledGaugeName, () -> flowGauges.get(spec.getUri().toString()).state.value);
        RootMetricContext.get().register(flowCompiledGaugeName, gauge);
      }

      //If the FlowSpec disallows concurrent executions, then check if another instance of the flow is already
      //running. If so, return immediately.
      boolean allowConcurrentExecution = ConfigUtils.getBoolean(flowConfig, ConfigurationKeys.FLOW_ALLOW_CONCURRENT_EXECUTION, true);

      if (!canRun(flowName, flowGroup, allowConcurrentExecution)) {
        _log.warn("Another instance of flowGroup: {}, flowName: {} running; Skipping flow execution since "
            + "concurrent executions are disabled for this flow.", flowGroup, flowName);
        flowGauges.get(spec.getUri().toString()).setState(CompiledState.FAILED);
        return;
      } else {
        flowGauges.get(spec.getUri().toString()).setState(CompiledState.SUCCESSFUL);
      }

      Dag<JobExecutionPlan> jobExecutionPlanDag = specCompiler.compileFlow(spec);

      Map<String, String> flowMetadata = TimingEventUtils.getFlowMetadata((FlowSpec) spec);
      if (jobExecutionPlanDag == null || jobExecutionPlanDag.isEmpty()) {
        // For scheduled flows, we do not insert the flowExecutionId into the FlowSpec. As a result, if the flow
        // compilation fails (i.e. we are unable to find a path), the metadata will not have flowExecutionId.
        // In this case, the current time is used as the flow executionId.
        flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
            Long.toString(System.currentTimeMillis()));
        TimingEvent flowCompileFailedTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get()
            .getTimingEvent(TimingEvent.FlowTimings.FLOW_COMPILE_FAILED) : null;
        Instrumented.markMeter(this.flowOrchestrationFailedMeter);
        _log.warn("Cannot determine an executor to run on for Spec: " + spec);
        if (flowCompileFailedTimer != null) {
          flowCompileFailedTimer.stop(flowMetadata);
        }
        return;
      }

      //If it is a scheduled flow (and hence, does not have flowExecutionId in the FlowSpec) and the flow compilation is successful,
      // retrieve the flowExecutionId from the JobSpec.
      flowMetadata.putIfAbsent(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD,
          jobExecutionPlanDag.getNodes().get(0).getValue().getJobSpec().getConfigAsProperties().getProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY));

      if (flowCompilationTimer != null) {
        flowCompilationTimer.stop(flowMetadata);
      }


      if (this.dagManager.isPresent()) {
        //Send the dag to the DagManager.
        this.dagManager.get().addDag(jobExecutionPlanDag, true);
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

            TimingEvent jobOrchestrationTimer = this.eventSubmitter.isPresent() ? this.eventSubmitter.get().
                getTimingEvent(TimingEvent.LauncherTimings.JOB_ORCHESTRATED) : null;

            producer.addSpec(jobSpec);

            if (jobOrchestrationTimer != null) {
              jobOrchestrationTimer.stop(jobMetadata);
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
      if (this.dagManager.isPresent()) {
        //Send the dag to the DagManager.
        _log.info("Forwarding cancel request for flow URI {} to DagManager.", spec.getUri());
        this.dagManager.get().stopDag(spec.getUri());
      } else {
        _log.warn("Operation not supported.");
      }
    } else {
      throw new RuntimeException("Spec not of type FlowSpec, cannot delete: " + spec);
    }
  }

  private FlowStatusGenerator buildFlowStatusGenerator(Config config) {
    JobStatusRetriever jobStatusRetriever;
    try {
      Class jobStatusRetrieverClass = Class.forName(ConfigUtils.getString(config, JOB_STATUS_RETRIEVER_CLASS_KEY, FsJobStatusRetriever.class.getName()));
      jobStatusRetriever =
          (JobStatusRetriever) GobblinConstructorUtils.invokeLongestConstructor(jobStatusRetrieverClass, config);
    } catch (ReflectiveOperationException e) {
      _log.error("Exception encountered when instantiating JobStatusRetriever");
      throw new RuntimeException(e);
    }
    return FlowStatusGenerator.builder().jobStatusRetriever(jobStatusRetriever).build();
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
    SUCCESSFUL(1);

    public int value;

    CompiledState(int value) {
      this.value = value;
    }
  }
}