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

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceLevel;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PathKeysParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;


/**
 * Resource for handling flow execution requests
 */
@RestLiCollection(name = "flowexecutions", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowExecutionResource extends ComplexKeyResourceTemplate<FlowStatusId, EmptyRecord, FlowExecution> {

  @Inject
  FlowExecutionResourceHandler flowExecutionResourceHandler;

  public FlowExecutionResource() {}

  /**
   * Retrieve the FlowExecution with the given key
   * @param key {@link FlowStatusId} of flow to get
   * @return corresponding {@link FlowExecution}
   */
  @Override
  public FlowExecution get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    return this.flowExecutionResourceHandler.get(key);
  }

  /**
   * Retrieve the most recent matching FlowExecution(s) of the identified FlowId
   * @param includeIssues include job issues in the response. Otherwise empty array of issues will be returned.
   */
  @Finder("latestFlowExecution")
  public List<FlowExecution> getLatestFlowExecution(@Context PagingContext context, @QueryParam("flowId") FlowId flowId,
      @Optional @QueryParam("count") Integer count, @Optional @QueryParam("tag") String tag, @Optional @QueryParam("executionStatus") String executionStatus,
      @Optional("false") @QueryParam("includeIssues") Boolean includeIssues) {
    return this.flowExecutionResourceHandler.getLatestFlowExecution(context, flowId, count, tag, executionStatus, includeIssues);
  }

  /**
   * Retrieve the most recent matching FlowExecution(s) for each flow in the identified flowGroup
   * @param countPerFlow (maximum) number of FlowExecutions for each flow in flowGroup   *
   * @param includeIssues include job issues in the response. Otherwise empty array of issues will be returned.
   * @return
   */
  @Finder("latestFlowGroupExecutions")
  public List<FlowExecution> getLatestFlowGroupExecutions(@Context PagingContext context, @QueryParam("flowGroup") String flowGroup,
      @Optional @QueryParam("countPerFlow") Integer countPerFlow, @Optional @QueryParam("tag") String tag,
      @Optional("false") @QueryParam("includeIssues") Boolean includeIssues) {
    return this.flowExecutionResourceHandler.getLatestFlowGroupExecutions(context, flowGroup, countPerFlow, tag, includeIssues);
  }

  /**
   * Resume a failed {@link FlowExecution} from the point before failure.
   * @param pathKeys key of {@link FlowExecution} specified in path
   */
  @Action(name="resume",resourceLevel= ResourceLevel.ENTITY)
  public void resume(@PathKeysParam PathKeys pathKeys) {
    this.flowExecutionResourceHandler.resume(pathKeys.get("id"));
  }

  /**
   * Kill the FlowExecution with the given key
   * @param key {@link FlowStatusId} of flow to kill
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    return this.flowExecutionResourceHandler.delete(key);
  }
}

