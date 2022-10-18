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

import java.util.List;

import org.apache.helix.HelixManager;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.UpdateResponse;

import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.FlowExecution;
import org.apache.gobblin.service.FlowExecutionResourceHandler;
import org.apache.gobblin.service.FlowExecutionResourceLocalHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.FlowStatusId;
import org.apache.gobblin.service.modules.core.GobblinServiceManager;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.service.monitoring.KillFlowEvent;
import org.apache.gobblin.service.monitoring.ResumeFlowEvent;


/**
 * {@link FlowExecutionResourceHandler} that calls underlying resource handler, but does extra work that requires objects
 * like the {@link HelixManager}. For now, that is just checking leadership and sending the kill through the eventBus
 * for the delete method.
 */
@Slf4j
public class GobblinServiceFlowExecutionResourceHandler implements FlowExecutionResourceHandler {
  private FlowExecutionResourceLocalHandler localHandler;
  private EventBus eventBus;
  private Optional<HelixManager> helixManager;
  private boolean forceLeader;

  @Inject
  public GobblinServiceFlowExecutionResourceHandler(FlowExecutionResourceLocalHandler handler,
      @Named(GobblinServiceManager.SERVICE_EVENT_BUS_NAME) EventBus eventBus,
      Optional<HelixManager> manager, @Named(InjectionNames.FORCE_LEADER) boolean forceLeader) {
    this.localHandler = handler;
    this.eventBus = eventBus;
    this.helixManager = manager;
    this.forceLeader = forceLeader;
  }

  @Override
  public FlowExecution get(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    return this.localHandler.get(key);
  }

  @Override
  public List<FlowExecution> getLatestFlowExecution(PagingContext context, FlowId flowId, Integer count, String tag,
      String executionStatus, Boolean includeIssues) {
    return this.localHandler.getLatestFlowExecution(context, flowId, count, tag, executionStatus, includeIssues);
  }

  @Override
  public List<FlowExecution> getLatestFlowGroupExecutions(PagingContext context, String flowGroup, Integer countPerFlow,
      String tag, Boolean includeIssues) {
    return this.localHandler.getLatestFlowGroupExecutions(context, flowGroup, countPerFlow, tag, includeIssues);
  }

  @Override
  public void resume(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    Long flowExecutionId = key.getKey().getFlowExecutionId();
    if (this.forceLeader) {
      HelixUtils.throwErrorIfNotLeader(this.helixManager);
    }
    this.eventBus.post(new ResumeFlowEvent(flowGroup, flowName, flowExecutionId));
  }

  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    Long flowExecutionId = key.getKey().getFlowExecutionId();
    if (this.forceLeader) {
      HelixUtils.throwErrorIfNotLeader(this.helixManager);
    }
    this.eventBus.post(new KillFlowEvent(flowGroup, flowName, flowExecutionId));
    return new UpdateResponse(HttpStatus.S_200_OK);
  }
}
