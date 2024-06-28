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
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.task.DagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;

/**
 * An abstract implementation for {@link DagProc} that enforces deadline for jobs.
 */
@Slf4j
abstract public class DeadlineEnforcementDagProc extends DagProc<Optional<Dag<JobExecutionPlan>>> {

  public DeadlineEnforcementDagProc(DagTask dagTask) {
    super(dagTask);
  }

  @Override
  protected Optional<Dag<JobExecutionPlan>> initialize(DagManagementStateStore dagManagementStateStore)
      throws IOException {
    return dagManagementStateStore.getDag(getDagId());
  }

  @Override
  protected void act(DagManagementStateStore dagManagementStateStore, Optional<Dag<JobExecutionPlan>> dag)
      throws IOException {
    if (validate(dag, dagManagementStateStore)) {
      enforceDeadline(dagManagementStateStore, dag.get());
    }
  }

  private boolean validate(Optional<Dag<JobExecutionPlan>> dag, DagManagementStateStore dagManagementStateStore) throws IOException {
    log.info("Request to enforce deadlines for dag {}", getDagId());
    DagActionStore.DagAction dagAction = getDagTask().getDagAction();

    if (!dag.isPresent()) {
      // todo - add a metric here
      log.error("Dag not present when validating {}. It may already have cancelled/finished. Dag {}",
          getDagId(), dagAction);
      return false;
    }

    if (!dagManagementStateStore.existsJobDagAction(dagAction.getFlowGroup(), dagAction.getFlowName(),
        dagAction.getFlowExecutionId(), dagAction.getJobName(), dagAction.getDagActionType())) {
      log.warn("Dag action {} is cleaned up from DMSS. No further action is required.", dagAction);
      return false;
    }

    return true;
  }

  abstract void enforceDeadline(DagManagementStateStore dagManagementStateStore, Dag<JobExecutionPlan> dag) throws IOException;
}
