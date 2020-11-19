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

package org.apache.gobblin.service;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import org.apache.gobblin.service.monitoring.FlowStatusGenerator;


/**
 * Resource for handling flow status requests
 */
@RestLiCollection(name = "flowstatuses", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowStatusResource extends ComplexKeyResourceTemplate<FlowStatusId, EmptyRecord, FlowStatus> {
  public static final String FLOW_STATUS_GENERATOR_INJECT_NAME = "FlowStatusGenerator";
  public static final String MESSAGE_SEPARATOR = ", ";

  @Inject @javax.inject.Inject @javax.inject.Named(FLOW_STATUS_GENERATOR_INJECT_NAME)
  FlowStatusGenerator _flowStatusGenerator;

  public FlowStatusResource() {}

  /**
   * Retrieve the FlowStatus with the given key
   * @param key flow status id key containing group name and flow name
   * @return {@link FlowStatus} with flow status for the latest execution of the flow
   */
  @Override
  public FlowStatus get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    // this returns null to raise a 404 error if flowStatus is null
    return convertFlowStatus(FlowExecutionResourceLocalHandler.getFlowStatusFromGenerator(key, this._flowStatusGenerator));
  }

  @Finder("latestFlowStatus")
  public List<FlowStatus> getLatestFlowStatus(@Context PagingContext context,
      @QueryParam("flowId") FlowId flowId, @Optional @QueryParam("count") Integer count, @Optional @QueryParam("tag") String tag) {
    List<org.apache.gobblin.service.monitoring.FlowStatus> flowStatuses = FlowExecutionResourceLocalHandler
        .getLatestFlowStatusesFromGenerator(flowId, count, tag, null, this._flowStatusGenerator);

    if (flowStatuses != null) {
      return flowStatuses.stream().map(this::convertFlowStatus).collect(Collectors.toList());
    }

    // will return 404 status code
    return null;
  }

  /**
   * Forms a {@link org.apache.gobblin.service.FlowStatus} from a {@link org.apache.gobblin.service.monitoring.FlowStatus}
   * Logic is used from {@link FlowExecutionResource} since this class is deprecated
   * @param monitoringFlowStatus
   * @return a {@link org.apache.gobblin.service.FlowStatus} converted from a {@link org.apache.gobblin.service.monitoring.FlowStatus}
   */
  private FlowStatus convertFlowStatus(org.apache.gobblin.service.monitoring.FlowStatus monitoringFlowStatus) {
    FlowExecution flowExecution = FlowExecutionResourceLocalHandler.convertFlowStatus(monitoringFlowStatus);
    return new FlowStatus()
        .setId(flowExecution.getId())
        .setExecutionStatistics(flowExecution.getExecutionStatistics())
        .setMessage(flowExecution.getMessage())
        .setExecutionStatus(flowExecution.getExecutionStatus())
        .setJobStatuses(flowExecution.getJobStatuses());
  }
}

