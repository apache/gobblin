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

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.LaunchDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;


/**
 * An implementation for {@link DagProc} that launches the start job of a flow.
 * If there are multiple start jobs for the flow, {@link ReevaluateDagProc} is created for each of them and that
 * launches those start jobs.
 * In a life cycle of a flow, {@link LaunchDagProc} runs only one time, unless it fails and is
 * retried by the retry-reminders. Post-launch, a subsequent {@link ReevaluateDagProc} runs after each job of the DAG
 * completes. That then may launch further jobs or conclude execution of the overall DAG (flow).
 */
@Slf4j
public class LaunchDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {
  private final FlowCompilationValidationHelper flowCompilationValidationHelper;

  public LaunchDagProc(LaunchDagTask launchDagTask, FlowCompilationValidationHelper flowCompilationValidationHelper,
      Config config) {
    super(launchDagTask, config);
    this.flowCompilationValidationHelper = flowCompilationValidationHelper;
  }

  /**
   * It retrieves the {@link FlowSpec} for the dag this dag proc corresponds to, from the {@link DagManagementStateStore}
   * and compiles it to create a {@link Dag} and saves it in the {@link DagManagementStateStore}.
   */
  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    try {
      FlowSpec flowSpec = dagManagementStateStore.getFlowSpec(FlowSpec.Utils.createFlowSpecUri(getDagId().getFlowId()));
      flowSpec.addProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, getDagId().getFlowExecutionId());
      Optional<Dag<JobExecutionPlan>> dag = this.flowCompilationValidationHelper.createExecutionPlanIfValid(flowSpec).toJavaUtil();
      if (dag.isPresent()) {
        dagManagementStateStore.addDag(dag.get());
      }
      return dag;
    } catch (URISyntaxException | SpecNotFoundException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag,
      DagProcessingEngineMetrics dagProcEngineMetrics) throws IOException {
    if (!dag.isPresent()) {
      log.warn("Dag with id " + getDagId() + " could not be compiled.");
      dagProcEngineMetrics.markDagActionsAct(getDagActionType(), false);
    } else {
      DagProcUtils.submitNextNodes(dagManagementStateStore, dag.get(), getDagId());
      DagProcUtils.setAndEmitFlowEvent(eventSubmitter, dag.get(), TimingEvent.FlowTimings.FLOW_RUNNING);
      dagManagementStateStore.getDagManagerMetrics().conditionallyMarkFlowAsState(DagUtils.getFlowId(dag.get()),
          Dag.FlowState.RUNNING);
      DagProcUtils.sendEnforceFlowFinishDeadlineDagAction(dagManagementStateStore, getDagTask().getDagAction());
      dagProcEngineMetrics.markDagActionsAct(getDagActionType(), true);
    }
  }
}
