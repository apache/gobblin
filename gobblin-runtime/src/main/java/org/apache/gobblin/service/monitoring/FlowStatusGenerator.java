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

package org.apache.gobblin.service.monitoring;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Builder;

import org.apache.gobblin.annotation.Alpha;


/**
 * Generator for {@link FlowStatus}, which relies on a {@link JobStatusRetriever}.
 */
@Alpha
@Builder
public class FlowStatusGenerator {
  private final JobStatusRetriever jobStatusRetriever;

  /**
   * Get the flow statuses of last <code>count</code> (or fewer) executions
   * @param flowName
   * @param flowGroup
   * @param count
   * @return the latest <code>count</code>{@link FlowStatus}es. null is returned if there is no flow execution found.
   */
  public List<FlowStatus> getLatestFlowStatus(String flowName, String flowGroup, int count) {
    List<Long> flowExecutionIds = jobStatusRetriever.getLatestExecutionIdsForFlow(flowName, flowGroup, count);

    if (flowExecutionIds == null || flowExecutionIds.isEmpty()) {
      return null;
    }
    List<FlowStatus> flowStatuses =
        flowExecutionIds.stream().map(flowExecutionId -> getFlowStatus(flowName, flowGroup, flowExecutionId))
            .collect(Collectors.toList());

    return flowStatuses;
  }

  /**
   * Get the flow status for a specific execution.
   * @param flowName
   * @param flowGroup
   * @param flowExecutionId
   * @return the flow status, null is returned if the flow status does not exist
   */
  public FlowStatus getFlowStatus(String flowName, String flowGroup, long flowExecutionId) {
    FlowStatus flowStatus = null;
    Iterator<JobStatus> jobStatusIterator =
        jobStatusRetriever.getJobStatusesForFlowExecution(flowName, flowGroup, flowExecutionId);

    if (jobStatusIterator.hasNext()) {
      flowStatus = new FlowStatus(flowName, flowGroup, flowExecutionId, jobStatusIterator);
    }

    return flowStatus;
  }
}
