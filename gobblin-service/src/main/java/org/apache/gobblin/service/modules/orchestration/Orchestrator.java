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
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Singleton;
import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumentable;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecCatalogListener;
import org.apache.gobblin.runtime.api.SpecProducer;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.TopologyCatalog;
import org.apache.gobblin.service.modules.flow.FlowUtils;
import org.apache.gobblin.service.modules.flow.SpecCompiler;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.modules.utils.SharedFlowMetricsSingleton;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;
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
  protected final TopologyCatalog topologyCatalog;
  private final JobStatusRetriever jobStatusRetriever;

  protected final MetricContext metricContext;

  protected final EventSubmitter eventSubmitter;
  @Getter
  private Meter flowOrchestrationSuccessFulMeter;
  @Getter
  private Meter flowOrchestrationFailedMeter;
  @Getter
  private Timer flowOrchestrationTimer;
  private final FlowLaunchHandler flowLaunchHandler;
  @Getter
  private final SharedFlowMetricsSingleton sharedFlowMetricsSingleton;

  @Inject
  public Orchestrator(Config config, TopologyCatalog topologyCatalog, Optional<Logger> log, FlowLaunchHandler flowLaunchHandler,
      SharedFlowMetricsSingleton sharedFlowMetricsSingleton, DagManagementStateStore dagManagementStateStore,
      FlowCompilationValidationHelper flowCompilationValidationHelper, JobStatusRetriever jobStatusRetriever) throws IOException {
    _log = log.isPresent() ? log.get() : LoggerFactory.getLogger(getClass());
    this.topologyCatalog = topologyCatalog;
    this.flowLaunchHandler = flowLaunchHandler;
    this.sharedFlowMetricsSingleton = sharedFlowMetricsSingleton;
    this.jobStatusRetriever = jobStatusRetriever;
    this.specCompiler = flowCompilationValidationHelper.getSpecCompiler();
    //At this point, the TopologySpecMap is initialized by the SpecCompiler. Pass the TopologySpecMap to the DagManager.
    ((MySqlDagManagementStateStore) dagManagementStateStore).setTopologySpecMap(getSpecCompiler().getTopologySpecMap());

    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), this.specCompiler.getClass());
    this.flowOrchestrationSuccessFulMeter = this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_SUCCESSFUL_METER);
    this.flowOrchestrationFailedMeter = this.metricContext.meter(ServiceMetricNames.FLOW_ORCHESTRATION_FAILED_METER);
    this.flowOrchestrationTimer = this.metricContext.timer(ServiceMetricNames.FLOW_ORCHESTRATION_TIMER);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, "org.apache.gobblin.service").build();
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
    return new AddSpecResponse<>(null);
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
      DagActionStore.DagAction launchDagAction = DagActionStore.DagAction.forFlow(flowGroup, flowName,
          FlowUtils.getOrCreateFlowExecutionId(flowSpec), DagActionStore.DagActionType.LAUNCH);
      DagActionStore.LeaseParams
          leaseObject = new DagActionStore.LeaseParams(launchDagAction, isReminderEvent,
          triggerTimestampMillis);
      // `flowSpec.isScheduled()` ==> adopt consensus `flowExecutionId` as clock drift safeguard, yet w/o disrupting API-layer's ad hoc ID assignment
      flowLaunchHandler.handleFlowLaunchTriggerEvent(jobProps, leaseObject, flowSpec.isScheduled());
      _log.info("Multi-active scheduler finished handling trigger event: [{}, is: {}, triggerEventTimestamp: {}]",
          launchDagAction, isReminderEvent ? "reminder" : "original", triggerTimestampMillis);
    } else {
      Instrumented.markMeter(this.flowOrchestrationFailedMeter);
      throw new RuntimeException("Spec not of type FlowSpec, cannot orchestrate: " + spec);
    }
    Instrumented.markMeter(this.flowOrchestrationSuccessFulMeter);
    Instrumented.updateTimer(this.flowOrchestrationTimer, System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
  }

  public void remove(Spec spec, Properties headers) throws IOException {
    URI uri = spec.getUri();
    // TODO: Evolve logic to cache and reuse previously compiled JobSpecs
    // .. this will work for Identity compiler but not always for multi-hop.
    // Note: Current logic assumes compilation is consistent between all executions
    if (spec instanceof FlowSpec) {
      String flowGroup = FlowSpec.Utils.getFlowGroup(uri);
      String flowName = FlowSpec.Utils.getFlowName(uri);
      List<Long> flowExecutionIds = this.jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, 10);
      _log.info("Found {} flows to cancel.", flowExecutionIds.size());

      for (long flowExecutionId : flowExecutionIds) {
      DagActionStore.DagAction killDagAction = DagActionStore.DagAction.forFlow(flowGroup, flowName, flowExecutionId,
          DagActionStore.DagActionType.KILL);
      DagActionStore.LeaseParams leaseParams = new DagActionStore.LeaseParams(killDagAction, false,
          System.currentTimeMillis());
      flowLaunchHandler.handleFlowKillTriggerEvent(new Properties(), leaseParams);
      }
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