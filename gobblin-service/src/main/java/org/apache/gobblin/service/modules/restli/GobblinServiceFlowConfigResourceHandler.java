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
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;

import com.google.common.base.Optional;
import com.linkedin.data.transform.DataProcessingException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.util.PatchApplier;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.api.FlowSpecSearchObject;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigLoggedException;
import org.apache.gobblin.service.FlowConfigResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigsResourceHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;
import org.apache.gobblin.service.modules.utils.HelixUtils;


/**
 * An HA (high available) aware {@link FlowConfigsResourceHandler} which consider if current node is Active or Standby.
 * When a Standby mode detected, it will forward the rest-li request ({@link FlowConfig})
 * to the Active. Otherwise it will handle it locally.
 */
@Slf4j
public class GobblinServiceFlowConfigResourceHandler implements FlowConfigsResourceHandler {
  @Getter
  private String serviceName;
  private boolean flowCatalogLocalCommit;
  private FlowConfigResourceLocalHandler localHandler;
  private Optional<HelixManager> helixManager;
  private GobblinServiceJobScheduler jobScheduler;
  private boolean forceLeader;

  public GobblinServiceFlowConfigResourceHandler(String serviceName, boolean flowCatalogLocalCommit,
      FlowConfigResourceLocalHandler handler,
      Optional<HelixManager> manager,
      GobblinServiceJobScheduler jobScheduler,
      boolean forceLeader) {
    this.flowCatalogLocalCommit = flowCatalogLocalCommit;
    this.serviceName = serviceName;
    this.localHandler = handler;
    this.helixManager = manager;
    this.jobScheduler = jobScheduler;
    this.forceLeader = forceLeader;
  }

  @Override
  public FlowConfig getFlowConfig(FlowId flowId)
      throws FlowConfigLoggedException {
    return this.localHandler.getFlowConfig(flowId);
  }

  @Override
  public Collection<FlowConfig> getFlowConfig(FlowSpecSearchObject flowSpecSearchObject) throws FlowConfigLoggedException {
    return this.localHandler.getFlowConfig(flowSpecSearchObject);
  }

  @Override
  public Collection<FlowConfig> getAllFlowConfigs() {
    return this.localHandler.getAllFlowConfigs();
  }

  /**
   * Adding {@link FlowConfig} should check if current node is active (master).
   * If current node is active, call {@link FlowConfigResourceLocalHandler#createFlowConfig(FlowConfig)} directly.
   * If current node is standby, forward {@link ServiceConfigKeys#HELIX_FLOWSPEC_ADD} to active. The remote active will
   * then call {@link FlowConfigResourceLocalHandler#createFlowConfig(FlowConfig)}.
   *
   * Please refer to {@link org.apache.gobblin.service.modules.core.ControllerUserDefinedMessageHandlerFactory} for remote handling.
   *
   * For better I/O load balance, user can enable {@link GobblinServiceFlowConfigResourceHandler#flowCatalogLocalCommit}.
   * The {@link FlowConfig} will be then persisted to {@link org.apache.gobblin.runtime.spec_catalog.FlowCatalog} first before it is
   * forwarded to active node (if current node is standby) for execution.
   */
  @Override
  public CreateResponse createFlowConfig(FlowConfig flowConfig)
      throws FlowConfigLoggedException {
    String flowName = flowConfig.getId().getFlowName();
    String flowGroup = flowConfig.getId().getFlowGroup();

    if (flowConfig.getProperties().containsKey(ConfigurationKeys.FLOW_EXECUTION_ID_KEY)) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          String.format("%s cannot be set by the user", ConfigurationKeys.FLOW_EXECUTION_ID_KEY),
          null);
    }

    checkHelixConnection(ServiceConfigKeys.HELIX_FLOWSPEC_ADD, flowName, flowGroup);

    if (forceLeader) {
      HelixUtils.throwErrorIfNotLeader(helixManager);
    }

    try {
      if (!jobScheduler.isActive() && helixManager.isPresent()) {
        CreateResponse response = null;
        if (this.flowCatalogLocalCommit) {
          // We will handle FS I/O locally for load balance before forwarding to remote node.
          response = this.localHandler.createFlowConfig(flowConfig, true);
        }

        if (!flowConfig.hasExplain() || !flowConfig.isExplain()) {
          //Forward the message to master only if it is not an "explain" request.
          forwardMessage(ServiceConfigKeys.HELIX_FLOWSPEC_ADD, FlowConfigUtils.serializeFlowConfig(flowConfig), flowName, flowGroup);
        }

        // Do actual work on remote node, directly return success
        return response == null ? new CreateResponse(new ComplexResourceKey<>(flowConfig.getId(), new EmptyRecord()),
            HttpStatus.S_201_CREATED) : response;
      } else {
        return this.localHandler.createFlowConfig(flowConfig);
      }
    } catch (IOException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Cannot create flowConfig [flowName=" + flowName + " flowGroup=" + flowGroup + "]", e);
    }
  }

  /**
   * Updating {@link FlowConfig} should check if current node is active (master).
   * If current node is active, call {@link FlowConfigResourceLocalHandler#updateFlowConfig(FlowId, FlowConfig)} directly.
   * If current node is standby, forward {@link ServiceConfigKeys#HELIX_FLOWSPEC_UPDATE} to active. The remote active will
   * then call {@link FlowConfigResourceLocalHandler#updateFlowConfig(FlowId, FlowConfig)}.
   *
   * Please refer to {@link org.apache.gobblin.service.modules.core.ControllerUserDefinedMessageHandlerFactory} for remote handling.
   *
   * For better I/O load balance, user can enable {@link GobblinServiceFlowConfigResourceHandler#flowCatalogLocalCommit}.
   * The {@link FlowConfig} will be then persisted to {@link org.apache.gobblin.runtime.spec_catalog.FlowCatalog} first before it is
   * forwarded to active node (if current node is standby) for execution.
   */
  @Override
  public UpdateResponse updateFlowConfig(FlowId flowId, FlowConfig flowConfig)
      throws FlowConfigLoggedException {
    String flowName = flowId.getFlowName();
    String flowGroup = flowId.getFlowGroup();

    if (!flowGroup.equals(flowConfig.getId().getFlowGroup()) || !flowName.equals(flowConfig.getId().getFlowName())) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST,
          "flowName and flowGroup cannot be changed in update", null);
    }

    checkHelixConnection(ServiceConfigKeys.HELIX_FLOWSPEC_UPDATE, flowName, flowGroup);

    if (forceLeader) {
      HelixUtils.throwErrorIfNotLeader(helixManager);
    }

    try {
      if (!jobScheduler.isActive() && helixManager.isPresent()) {

        if (this.flowCatalogLocalCommit) {
          // We will handle FS I/O locally for load balance before forwarding to remote node.
          this.localHandler.updateFlowConfig(flowId, flowConfig, false);
        }

        forwardMessage(ServiceConfigKeys.HELIX_FLOWSPEC_UPDATE, FlowConfigUtils.serializeFlowConfig(flowConfig), flowName, flowGroup);

        // Do actual work on remote node, directly return success
        log.info("Forwarding update flowConfig [flowName=" + flowName + " flowGroup=" + flowGroup + "]");
        return new UpdateResponse(HttpStatus.S_200_OK);
      } else {
        return this.localHandler.updateFlowConfig(flowId, flowConfig);
      }

    } catch (IOException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Cannot update flowConfig [flowName=" + flowName + " flowGroup=" + flowGroup + "]", e);
    }
  }

  @Override
  public UpdateResponse partialUpdateFlowConfig(FlowId flowId, PatchRequest<FlowConfig> flowConfigPatch) {
    FlowConfig flowConfig = getFlowConfig(flowId);

    try {
      PatchApplier.applyPatch(flowConfig, flowConfigPatch);
    } catch (DataProcessingException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "Failed to apply partial update", e);
    }

    return updateFlowConfig(flowId, flowConfig);
  }

  /**
   * Deleting {@link FlowConfig} should check if current node is active (master).
   * If current node is active, call {@link FlowConfigResourceLocalHandler#deleteFlowConfig(FlowId, Properties)} directly.
   * If current node is standby, forward {@link ServiceConfigKeys#HELIX_FLOWSPEC_REMOVE} to active. The remote active will
   * then call {@link FlowConfigResourceLocalHandler#deleteFlowConfig(FlowId, Properties)}.
   *
   * Please refer to {@link org.apache.gobblin.service.modules.core.ControllerUserDefinedMessageHandlerFactory} for remote handling.
   *
   * For better I/O load balance, user can enable {@link GobblinServiceFlowConfigResourceHandler#flowCatalogLocalCommit}.
   * The {@link FlowConfig} will be then persisted to {@link org.apache.gobblin.runtime.spec_catalog.FlowCatalog} first before it is
   * forwarded to active node (if current node is standby) for execution.
   */
  @Override
  public UpdateResponse deleteFlowConfig(FlowId flowId, Properties header)
      throws FlowConfigLoggedException {
    String flowName = flowId.getFlowName();
    String flowGroup = flowId.getFlowGroup();

    checkHelixConnection(ServiceConfigKeys.HELIX_FLOWSPEC_REMOVE, flowName, flowGroup);

    if (forceLeader) {
      HelixUtils.throwErrorIfNotLeader(helixManager);
    }

    try {
      if (!jobScheduler.isActive() && helixManager.isPresent()) {

        if (this.flowCatalogLocalCommit) {
          // We will handle FS I/O locally for load balance before forwarding to remote node.
          this.localHandler.deleteFlowConfig(flowId, header, false);
        }

        forwardMessage(ServiceConfigKeys.HELIX_FLOWSPEC_REMOVE, FlowConfigUtils.serializeFlowId(flowId), flowName, flowGroup);

        return new UpdateResponse(HttpStatus.S_200_OK);
      } else {
        return this.localHandler.deleteFlowConfig(flowId, header);
      }
    } catch (IOException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Cannot delete flowConfig [flowName=" + flowName + " flowGroup=" + flowGroup + "]", e);
    }
  }

  private void checkHelixConnection(String opr, String flowName, String flowGroup) throws FlowConfigLoggedException {
    if (this.helixManager.isPresent() && !this.helixManager.get().isConnected()) {
      // Specs in store will be notified when Scheduler is added as listener to FlowCatalog, so ignore
      // .. Specs if in cluster mode and Helix is not yet initialized
      log.warn("System not yet initialized. Skipping operation " + opr);
      throw new FlowConfigLoggedException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "System not yet initialized. Skipping " + opr + " flowConfig [flowName=" + flowName + " flowGroup=" + flowGroup + "]");
    }
  }

  private void forwardMessage(String msgSubType, String val, String flowName, String flowGroup) {
    HelixUtils.sendUserDefinedMessage(msgSubType, val, UUID.randomUUID().toString(), InstanceType.CONTROLLER,
        helixManager.get(), log);
    log.info("{} Forwarding {} flowConfig [flowName={} flowGroup={}", serviceName, msgSubType, flowName, flowGroup + "]");
  }
}
