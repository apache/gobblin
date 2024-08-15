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

package org.apache.gobblin.service.modules.orchestration.task;

import java.net.URISyntaxException;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.SpecNotFoundException;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.DagUtils;
import org.apache.gobblin.service.modules.orchestration.DagTaskVisitor;
import org.apache.gobblin.service.modules.orchestration.LeaseAttemptStatus;


/**
 * A {@link DagTask} responsible to handle launch tasks.
 */
@Slf4j
public class LaunchDagTask extends DagTask {
  public LaunchDagTask(DagActionStore.DagAction dagAction, LeaseAttemptStatus.LeaseObtainedStatus leaseObtainedStatus,
      DagManagementStateStore dagManagementStateStore, DagProcessingEngineMetrics dagProcEngineMetrics) {
    super(dagAction, leaseObtainedStatus, dagManagementStateStore, dagProcEngineMetrics);
  }

  public <T> T host(DagTaskVisitor<T> visitor) {
    return visitor.meet(this);
  }

  @Override
  public final boolean conclude() {
    try {
      // Remove adhoc flow specs after the adhoc job is launched and marked as completed
      if (super.conclude()) {
        Dag.DagId dagId = DagUtils.generateDagId(this.dagAction.getFlowGroup(),
            this.dagAction.getFlowName(), this.dagAction.getFlowExecutionId());
        FlowSpec flowSpec =
            this.dagManagementStateStore.getFlowSpec(FlowSpec.Utils.createFlowSpecUri(dagId.getFlowId()));
        if (!flowSpec.isScheduled()) {
          dagManagementStateStore.removeFlowSpec(flowSpec.getUri(), new Properties(), false);
        }
        return true;
      }
    } catch (SpecNotFoundException | URISyntaxException e) {
      log.error("Unable to retrieve flowSpec to delete from flowCatalog if adhoc.");
      throw new RuntimeException(e);
    }
    return false;
  }
}
