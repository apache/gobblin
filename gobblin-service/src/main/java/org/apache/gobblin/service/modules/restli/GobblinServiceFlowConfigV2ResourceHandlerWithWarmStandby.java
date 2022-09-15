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
import com.linkedin.data.transform.DataProcessingException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.util.PatchApplier;
import java.util.Properties;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigLoggedException;
import org.apache.gobblin.service.FlowConfigResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigV2ResourceLocalHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.runtime.util.InjectionNames;
import org.apache.helix.HelixManager;

@Slf4j
public class GobblinServiceFlowConfigV2ResourceHandlerWithWarmStandby extends GobblinServiceFlowConfigV2ResourceHandler {
 @Inject
  public GobblinServiceFlowConfigV2ResourceHandlerWithWarmStandby(@Named(InjectionNames.SERVICE_NAME) String serviceName,
      @Named(InjectionNames.FLOW_CATALOG_LOCAL_COMMIT) boolean flowCatalogLocalCommit,
      FlowConfigV2ResourceLocalHandler handler, Optional<HelixManager> manager, GobblinServiceJobScheduler jobScheduler,
      @Named(InjectionNames.FORCE_LEADER) boolean forceLeader) {
    super(serviceName, flowCatalogLocalCommit, handler, manager, jobScheduler, forceLeader);
  }


  @Override
  public UpdateResponse deleteFlowConfig(FlowId flowId, Properties header)
      throws FlowConfigLoggedException {
    return this.localHandler.deleteFlowConfig(flowId, header);
  }

  @Override
  public UpdateResponse  partialUpdateFlowConfig(FlowId flowId,
      PatchRequest<FlowConfig> flowConfigPatch) throws FlowConfigLoggedException {
    long modifiedWatermark = System.currentTimeMillis() / 1000;
    FlowConfig flowConfig = getFlowConfig(flowId);

    try {
      PatchApplier.applyPatch(flowConfig, flowConfigPatch);
    } catch (DataProcessingException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "Failed to apply partial update", e);
    }

    return updateFlowConfig(flowId, flowConfig, modifiedWatermark);
  }

  @Override
  public UpdateResponse updateFlowConfig(FlowId flowId,
      FlowConfig flowConfig) throws FlowConfigLoggedException {
    // We have modifiedWatermark here to avoid update config happens at the same time on different hosts overwrite each other
    // timestamp here will be treated as largest modifiedWatermark that we can update
    long version = System.currentTimeMillis() / 1000;
    return updateFlowConfig(flowId, flowConfig, version);
  }
  public UpdateResponse updateFlowConfig(FlowId flowId,
      FlowConfig flowConfig, long modifiedWatermark) throws FlowConfigLoggedException {
    String flowName = flowId.getFlowName();
    String flowGroup = flowId.getFlowGroup();

    if (!flowGroup.equals(flowConfig.getId().getFlowGroup()) || !flowName.equals(flowConfig.getId().getFlowName())) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          "flowName and flowGroup cannot be changed in update", null);
    }

      // We directly call localHandler to create flow config and put it in spec store

      //Instead of helix message, forwarding message is done by change stream of spec store

      return this.localHandler.updateFlowConfig(flowId, flowConfig, true, modifiedWatermark);
  }
  /**
   * Adding {@link FlowConfig} call {@link FlowConfigResourceLocalHandler#createFlowConfig(FlowConfig)} directly.
   * no matter it's active or standby, rely on the CDC stream for spec store to forward the change to other hosts
   *
   */
  @Override
  public CreateResponse createFlowConfig(FlowConfig flowConfig)
      throws FlowConfigLoggedException {

    if (flowConfig.getProperties().containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          String.format("%s cannot be set by the user", ConfigurationKeys.FLOW_EXECUTION_ID_KEY), null);
    }


    CreateResponse response = null;
    // We directly call localHandler to create flow config and put it in spec store
    response = this.localHandler.createFlowConfig(flowConfig, true);

    //Instead of helix message, forwarding message is done by change stream of spec store

    // Do actual work on remote node, directly return success

    return response == null ? new CreateResponse(new ComplexResourceKey<>(flowConfig.getId(), new EmptyRecord()),
        HttpStatus.S_201_CREATED) : response;

  }
}
