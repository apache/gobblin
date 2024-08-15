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

package org.apache.gobblin.service.modules.restli;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.FlowExecution;
import org.apache.gobblin.service.FlowExecutionResource;
import org.apache.gobblin.service.FlowExecutionResourceHandlerInterface;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.FlowStatusId;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.monitoring.FlowStatus;
import org.apache.gobblin.service.monitoring.FlowStatusGenerator;


@Slf4j
public class FlowExecutionResourceHandler implements FlowExecutionResourceHandlerInterface {
  private final DagManagementStateStore dagManagementStateStore;
  private final FlowStatusGenerator flowStatusGenerator;

  @Inject
  public FlowExecutionResourceHandler(FlowStatusGenerator flowStatusGenerator,
      DagManagementStateStore dagManagementStateStore) {
    this.dagManagementStateStore = dagManagementStateStore;
    this.flowStatusGenerator = flowStatusGenerator;
  }

  public FlowExecution get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    FlowExecution flowExecution = FlowExecutionResource.convertFlowStatus(FlowExecutionResource.getFlowStatusFromGenerator(key, this.flowStatusGenerator), true);
    if (flowExecution == null) {
      throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, "No flow execution found for flowStatusId " + key.getKey()
          + ". The flowStatusId may be incorrect, or the flow execution may have been cleaned up.");
    }
    return flowExecution;  }

  public List<FlowExecution> getLatestFlowExecution(PagingContext context, FlowId flowId, Integer count, String tag,
      String executionStatus, Boolean includeIssues) {
    List<FlowStatus> flowStatuses = FlowExecutionResource.getLatestFlowStatusesFromGenerator(flowId, count, tag, executionStatus, this.flowStatusGenerator);

    if (flowStatuses != null) {
      return flowStatuses.stream()
          .map((FlowStatus monitoringFlowStatus) -> FlowExecutionResource.convertFlowStatus(monitoringFlowStatus, includeIssues))
          .collect(Collectors.toList());
    }

    throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, "No flow execution found for flowId " + flowId
        + ". The flowId may be incorrect, the flow execution may have been cleaned up, or not matching tag (" + tag
        + ") and/or execution status (" + executionStatus + ").");
  }

  public List<FlowExecution> getLatestFlowGroupExecutions(PagingContext context, String flowGroup, Integer countPerFlow,
      String tag, Boolean includeIssues) {
    List<FlowStatus> flowStatuses =
        getLatestFlowGroupStatusesFromGenerator(flowGroup, countPerFlow, tag, this.flowStatusGenerator);

    if (flowStatuses != null) {
      // todo: flow end time will be incorrect when dag manager is not used
      //       and FLOW_SUCCEEDED/FLOW_CANCELLED/FlowFailed events are not sent
      return flowStatuses.stream()
          .map((FlowStatus monitoringFlowStatus) -> FlowExecutionResource.convertFlowStatus(monitoringFlowStatus, includeIssues))
          .collect(Collectors.toList());
    }

    throw new RestLiServiceException(HttpStatus.S_404_NOT_FOUND, "No flow executions found for flowGroup " + flowGroup
        + ". The group name may be incorrect, the flow execution may have been cleaned up, or not matching tag (" + tag
        + ").");
  }

  public void resume(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    FlowStatusId id = this.get(key).getId(); // pre-check to throw `HttpStatus.S_404_NOT_FOUND`, in case FlowExecution doesn't exist
    addDagAction(id.getFlowGroup(), id.getFlowName(), id.getFlowExecutionId(), DagActionStore.DagActionType.RESUME);
  }

  public UpdateResponse delete(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    FlowStatusId id = this.get(key).getId();  // pre-check to throw `HttpStatus.S_404_NOT_FOUND`, in case FlowExecution doesn't exist
    addDagAction(id.getFlowGroup(), id.getFlowName(), id.getFlowExecutionId(), DagActionStore.DagActionType.KILL);
    return new UpdateResponse(HttpStatus.S_200_OK);
  }

  /** NOTE: may throw {@link RestLiServiceException}, see: https://linkedin.github.io/rest.li/user_guide/restli_server#returning-errors */
  protected void addDagAction(String flowGroup, String flowName, Long flowExecutionId, DagActionStore.DagActionType actionType) {
    try {
      // If an existing resume request is still pending then do not accept this request
      if (this.dagManagementStateStore.existsFlowDagAction(flowGroup, flowName, flowExecutionId, actionType)) {
        this.throwErrorResponse("There is already a pending " + actionType + " action for this flow. Please wait to resubmit and wait "
            + "for action to be completed.", HttpStatus.S_409_CONFLICT);
        return;
      }
      this.dagManagementStateStore.addFlowDagAction(flowGroup, flowName, flowExecutionId, actionType);
    } catch (IOException | SQLException e) {
      log.warn(
          String.format("Failed to add %s action for flow %s %s %s to dag action store due to:", actionType, flowGroup,
              flowName, flowExecutionId), e);
      this.throwErrorResponse(e.getMessage(), HttpStatus.S_500_INTERNAL_SERVER_ERROR);
    }
  }

  private void throwErrorResponse(String exceptionMessage, HttpStatus errorType) {
    throw StringUtils.isBlank(exceptionMessage) ? new RestLiServiceException(errorType) : new RestLiServiceException(errorType, exceptionMessage);
  }

  public static List<FlowStatus> getLatestFlowGroupStatusesFromGenerator(String flowGroup,
      Integer countPerFlowName, String tag, FlowStatusGenerator flowStatusGenerator) {
    if (countPerFlowName == null) {
      countPerFlowName = 1;
    }
    log.info("get latest (for group) called with flowGroup " + flowGroup + " count " + countPerFlowName);

    return flowStatusGenerator.getFlowStatusesAcrossGroup(flowGroup, countPerFlowName, tag);
  }
}
