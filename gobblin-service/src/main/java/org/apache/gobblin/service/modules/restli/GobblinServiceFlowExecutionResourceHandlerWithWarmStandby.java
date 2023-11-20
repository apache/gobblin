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

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;
import java.io.IOException;
import java.sql.SQLException;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.runtime.api.DagActionStore;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.gobblin.service.FlowExecutionResourceLocalHandler;
import org.apache.gobblin.service.FlowStatusId;
import org.apache.gobblin.service.modules.core.GobblinServiceManager;
import org.apache.helix.HelixManager;

@Slf4j
public class GobblinServiceFlowExecutionResourceHandlerWithWarmStandby extends GobblinServiceFlowExecutionResourceHandler{
  private DagActionStore dagActionStore;
  @Inject
  public GobblinServiceFlowExecutionResourceHandlerWithWarmStandby(FlowExecutionResourceLocalHandler handler,
      @Named(GobblinServiceManager.SERVICE_EVENT_BUS_NAME) EventBus eventBus,
      Optional<HelixManager> manager, @Named(InjectionNames.FORCE_LEADER) boolean forceLeader, DagActionStore dagActionStore) {
    super(handler, eventBus, manager, forceLeader);
    this.dagActionStore = dagActionStore;
  }

  @Override
  public void resume(ComplexResourceKey<FlowStatusId, EmptyRecord> key) {
    FlowStatusId id = key.getKey();
    addDagAction(id.getFlowGroup(), id.getFlowName(), id.getFlowExecutionId(), DagActionStore.FlowActionType.RESUME);
  }

  @Override
  public UpdateResponse delete(ComplexResourceKey<org.apache.gobblin.service.FlowStatusId, EmptyRecord> key) {
    FlowStatusId id = key.getKey();
    addDagAction(id.getFlowGroup(), id.getFlowName(), id.getFlowExecutionId(), DagActionStore.FlowActionType.KILL);
    return new UpdateResponse(HttpStatus.S_200_OK);
  }

  /** NOTE: may throw {@link RestLiServiceException}, see: https://linkedin.github.io/rest.li/user_guide/restli_server#returning-errors */
  protected void addDagAction(String flowGroup, String flowName, Long flowExecutionId, DagActionStore.FlowActionType actionType) {
    try {
      // If an existing resume request is still pending then do not accept this request
      if (this.dagActionStore.exists(flowGroup, flowName, flowExecutionId.toString(), actionType)) {
        this.throwErrorResponse("There is already a pending " + actionType + " action for this flow. Please wait to resubmit and wait "
            + "for action to be completed.", HttpStatus.S_409_CONFLICT);
        return;
      }
      this.dagActionStore.addDagAction(flowGroup, flowName, flowExecutionId.toString(), actionType);
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
}
