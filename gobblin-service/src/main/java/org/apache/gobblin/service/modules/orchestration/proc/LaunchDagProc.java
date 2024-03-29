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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagManagerUtils;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;


/**
 * An implementation for {@link DagProc} that launches a new job.
 */
@Slf4j
public class LaunchDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {
  private final FlowCompilationValidationHelper flowCompilationValidationHelper;
  // todo - this is not orchestration delay and should be renamed. keeping it the same because DagManager is also using
  // the same name
  private static final AtomicLong orchestrationDelayCounter = new AtomicLong(0);

  static {
    metricContext.register(
        metricContext.newContextAwareGauge(ServiceMetricNames.FLOW_ORCHESTRATION_DELAY, orchestrationDelayCounter::get));
  }

  public LaunchDagProc(LaunchDagTask launchDagTask, FlowCompilationValidationHelper flowCompilationValidationHelper) {
    super(launchDagTask);
    this.flowCompilationValidationHelper = flowCompilationValidationHelper;
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    try {
      FlowSpec flowSpec = dagManagementStateStore.getFlowSpec(FlowSpec.Utils.createFlowSpecUri(getDagId().getFlowId()));
      flowSpec.addProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, getDagId().getFlowExecutionId());
      return this.flowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec).toJavaUtil();
    } catch (URISyntaxException | SpecNotFoundException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    if (!dag.isPresent()) {
      log.warn("Dag with id " + getDagId() + " could not be compiled.");
      // todo - add metrics
    } else {
      submitNextNodes(dagManagementStateStore, dag.get());
      orchestrationDelayCounter.set(System.currentTimeMillis() - DagManagerUtils.getFlowExecId(dag.get()));
    }
  }

  /**
   * Submit next set of Dag nodes in the provided Dag.
   */
   private void submitNextNodes(DagManagementStateStore dagManagementStateStore,
       Dag<JobExecutionPlan> dag) throws IOException {
     Set<Dag.DagNode<JobExecutionPlan>> nextNodes = DagManagerUtils.getNext(dag);

     if (nextNodes.size() > 1) {
       handleMultipleJobs(nextNodes);
     }

     //Submit jobs from the dag ready for execution.
     for (Dag.DagNode<JobExecutionPlan> dagNode : nextNodes) {
       DagProcUtils.submitJobToExecutor(dagManagementStateStore, dagNode, getDagId());
       log.info("Submitted job {} for dagId {}", DagManagerUtils.getJobName(dagNode), getDagId());
     }

     //Checkpoint the dag state, it should have an updated value of dag nodes
     dagManagementStateStore.checkpointDag(dag);
   }

  private void handleMultipleJobs(Set<Dag.DagNode<JobExecutionPlan>> nextNodes) {
     throw new UnsupportedOperationException("More than one start job is not allowed");
  }
}
