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

package org.apache.gobblin.service.modules.core;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MultiTypeMessageHandlerFactory;
import org.apache.helix.model.Message;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.service.FlowConfig;
import org.apache.gobblin.service.FlowConfigResourceLocalHandler;
import org.apache.gobblin.service.FlowConfigsResourceHandler;
import org.apache.gobblin.service.FlowId;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.restli.FlowConfigUtils;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;

/**
 * A custom {@link MultiTypeMessageHandlerFactory} for {@link org.apache.gobblin.service.modules.core.ControllerUserDefinedMessageHandlerFactory}s that
 * handle messages of type {@link org.apache.helix.model.Message.MessageType#USER_DEFINE_MSG}.
 */
@AllArgsConstructor
class ControllerUserDefinedMessageHandlerFactory implements MultiTypeMessageHandlerFactory {
  private final boolean flowCatalogLocalCommit;
  private final GobblinServiceJobScheduler jobScheduler;
  private final FlowConfigsResourceHandler resourceHandler;
  private final String serviceName;

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    return new ControllerUserDefinedMessageHandler(message, context, serviceName, flowCatalogLocalCommit, jobScheduler, resourceHandler);
  }

  @Override
  public String getMessageType() {
    return Message.MessageType.USER_DEFINE_MSG.toString();
  }

  public List<String> getMessageTypes() {
    return Collections.singletonList(getMessageType());
  }

  @Override
  public void reset() {

  }

  /**
   * A custom {@link MessageHandler} for handling user-defined messages to the controller.
   */
  @Slf4j
  private static class ControllerUserDefinedMessageHandler extends MessageHandler {
    private final boolean flowCatalogLocalCommit;
    private final GobblinServiceJobScheduler jobScheduler;
    private final FlowConfigsResourceHandler resourceHandler;
    private final String serviceName;

    public ControllerUserDefinedMessageHandler(Message message, NotificationContext context, String serviceName,
        boolean flowCatalogLocalCommit, GobblinServiceJobScheduler scheduler,
        FlowConfigsResourceHandler resourceHandler) {
      super(message, context);
      this.serviceName = serviceName;
      this.flowCatalogLocalCommit = flowCatalogLocalCommit;
      this.jobScheduler = scheduler;
      this.resourceHandler = resourceHandler;
    }

    /**
     * Method to handle add flow config message forwarded by Helix (Standby) node.
     * In load balance mode, the FlowCatalog I/O was handled on standby when receiving Restli, so only need to handle
     * {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onAddSpec(Spec)} part.
     * Otherwise, we have to handle both FlowCatalog I/O and {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onAddSpec(Spec)}.
     *
     * Please refer to {@link FlowConfigResourceLocalHandler#createFlowConfig(FlowConfig)}. It will handle both FlowCatalog I/O and
     * {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onAddSpec(Spec)} in non-balance mode.
     */
    private void handleAdd(String msg)
        throws IOException {
      FlowConfig config = FlowConfigUtils.deserializeFlowConfig(msg);
      if (this.flowCatalogLocalCommit) {
        // in balance mode, flow spec is already added in flow catalog on standby node.
        FlowSpec flowSpec = FlowConfigResourceLocalHandler.createFlowSpecForConfig(config);
        log.info("Only handle add {} scheduling because flow catalog is committed locally on standby.", flowSpec);
        jobScheduler.onAddSpec(flowSpec);
      } else {
        resourceHandler.createFlowConfig(config);
      }
    }

    /**
     * Method to handle add flow config message forwarded by Helix (Standby) node.
     * In load balance mode, the FlowCatalog I/O was handled on standby when receiving Restli, so only need to handle
     * {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onUpdateSpec(Spec)} part.
     * Otherwise, we have to handle both FlowCatalog I/O and {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onUpdateSpec(Spec)}.
     *
     * Please refer to {@link FlowConfigResourceLocalHandler#updateFlowConfig(FlowId, FlowConfig)}. It will handle both FlowCatalog I/O and
     * {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onUpdateSpec(Spec)} in non-balance mode.
     */
    private void handleUpdate(String msg)
        throws IOException {
      FlowConfig config = FlowConfigUtils.deserializeFlowConfig(msg);
      if (flowCatalogLocalCommit) {
        // in balance mode, flow spec is already updated in flow catalog on standby node.
        FlowSpec flowSpec = FlowConfigResourceLocalHandler.createFlowSpecForConfig(config);
        log.info("Only handle update {} scheduling because flow catalog is committed locally on standby.", flowSpec);
        jobScheduler.onUpdateSpec(flowSpec);
      } else {
        resourceHandler.updateFlowConfig(config.getId(), config);
      }
    }

    /**
     * Method to handle add flow config message forwarded by Helix (Standby) node.
     * In load balance mode, the FlowCatalog I/O was handled on standby when receiving Restli, so only need to handle
     * {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onDeleteSpec(URI, String, Properties)} part.
     * Otherwise, we have to handle both FlowCatalog I/O and {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onDeleteSpec(URI, String, Properties)}.
     *
     * Please refer to {@link FlowConfigResourceLocalHandler#deleteFlowConfig(FlowId, Properties)}. It will handle both FlowCatalog I/O and
     * {@link org.apache.gobblin.runtime.api.SpecCatalogListener#onDeleteSpec(URI, String, Properties)} in non-balance mode.
     */
    private void handleDelete(String msg)
        throws IOException {
      try {
        FlowId id = FlowConfigUtils.deserializeFlowId(msg);
        if (flowCatalogLocalCommit) {
          // in balance mode, flow spec is already deleted in flow catalog on standby node.
          URI flowUri = FlowSpec.Utils.createFlowSpecUri(id);
          log.info("Only handle update {} scheduling because flow catalog is committed locally on standby.", flowUri);
          jobScheduler.onDeleteSpec(flowUri, FlowSpec.Builder.DEFAULT_VERSION);
        } else {
          resourceHandler.deleteFlowConfig(id, new Properties());
        }
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }

    @Override
    public HelixTaskResult handleMessage()
        throws InterruptedException {
      if (jobScheduler.isActive()) {
        // we want to make sure current node is in active state
        String msg = _message.getAttribute(Message.Attributes.INNER_MESSAGE);
        log.info("{} ControllerUserDefinedMessage received : {}, type {}", this.serviceName, msg, _message.getMsgSubType());
        try {
          if (_message.getMsgSubType().equals(ServiceConfigKeys.HELIX_FLOWSPEC_ADD)) {
            handleAdd(msg);
          } else if (_message.getMsgSubType().equals(ServiceConfigKeys.HELIX_FLOWSPEC_REMOVE)) {
            handleDelete(msg);
          } else if (_message.getMsgSubType().equals(ServiceConfigKeys.HELIX_FLOWSPEC_UPDATE)) {
            handleUpdate(msg);
          }
        } catch (IOException e) {
          log.error("Cannot process Helix message.", e);
          HelixTaskResult helixTaskResult = new HelixTaskResult();
          helixTaskResult.setSuccess(false);
          return helixTaskResult;
        }
      } else {
        String msg = _message.getAttribute(Message.Attributes.INNER_MESSAGE);
        log.error("ControllerUserDefinedMessage received but ignored due to not in active mode: {}, type {}", msg,
            _message.getMsgSubType());
      }
      HelixTaskResult helixTaskResult = new HelixTaskResult();
      helixTaskResult.setSuccess(true);

      return helixTaskResult;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      log.error(
          String.format("Failed to handle message with exception %s, error code %s, error type %s", e, code, type));
    }
  }
}


