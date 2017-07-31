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

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;

import org.apache.gobblin.annotation.Alpha;

import lombok.Builder;


/**
 * Generator for {@link FlowStatus}, which relies on a {@link JobStatusRetriever}.
 */
@Alpha
@Builder
public class FlowStatusGenerator {
  private final JobStatusRetriever jobStatusRetriever;

  /**
   * Get the latest flow status.
   * @param flowName
   * @param flowGroup
   * @return the latest {@link FlowStatus}. null is returned if there is no flow execution found.
   */
  public FlowStatus getLatestFlowStatus(String flowName, String flowGroup) {
    FlowStatus flowStatus = null;
    long latestExecutionId = jobStatusRetriever.getLatestExecutionIdForFlow(flowName, flowGroup);

    if (latestExecutionId != -1l) {
      flowStatus = getFlowStatus(flowName, flowGroup, latestExecutionId);
    }

    return flowStatus;
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
