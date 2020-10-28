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

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.FlowExecution;
import org.apache.gobblin.service.FlowExecutionResourceHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.FlowStatusId;
import org.apache.gobblin.service.modules.utils.HelixUtils;
import org.apache.gobblin.service.monitoring.KillFlowEvent;


/**
 * {@link FlowExecutionResourceHandler} that calls underlying resource handler, but does extra work that requires objects
 * like the {@link HelixManager}. For now, that is just checking leadership and sending the kill through the eventBus
 * for the delete method.
 */
@Slf4j
public class GobblinServiceFlowExecutionResourceHandler implements FlowExecutionResourceHandler {
  private FlowExecutionResourceHandler localHandler;
  private EventBus eventBus;
  private Optional<HelixManager> helixManager;
  private boolean forceLeader;

  public GobblinServiceFlowExecutionResourceHandler(FlowExecutionResourceHandler handler, EventBus eventBus,
      Optional<HelixManager> manager, boolean forceLeader) {
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
  public List<FlowExecution> getLatestFlowExecution(PagingContext context, FlowId flowId, Integer count, String tag, String executionStatus) {
    return this.localHandler.getLatestFlowExecution(context, flowId, count, tag, executionStatus);
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
