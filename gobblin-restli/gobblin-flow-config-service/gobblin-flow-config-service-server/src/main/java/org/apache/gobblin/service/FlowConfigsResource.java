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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Resource for handling flow configuration requests
 */
@RestLiCollection(name = "flowconfigs", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowConfigsResource extends ComplexKeyResourceTemplate<FlowId, EmptyRecord, FlowConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowConfigsResource.class);

  public static final String INJECT_READY_TO_USE = "readToUse";

  private static final Set<String> ALLOWED_METADATA = ImmutableSet.of("delete.state.store");

  @Inject
  private FlowConfigsResourceHandler flowConfigsResourceHandler;

  // For getting who sends the request
  @Inject
  private RequesterService requesterService;

  // For blocking use of this resource until it is ready
  @Inject
  @Named(INJECT_READY_TO_USE)
  private Boolean readyToUse;

  public FlowConfigsResource() {
  }

  /**
   * Retrieve the flow configuration with the given key
   * @param key flow config id key containing group name and flow name
   * @return {@link FlowConfig} with flow configuration
   */
  @Override
  public FlowConfig get(ComplexResourceKey<FlowId, EmptyRecord> key) {
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.flowConfigsResourceHandler.getFlowConfig(flowId);
  }

  /**
   * Create a flow configuration that the service will forward to execution instances for execution
   * @param flowConfig flow configuration
   * @return {@link CreateResponse}
   */
  @Override
  public CreateResponse create(FlowConfig flowConfig) {
    if (flowConfig.hasOwningGroup()) {
      throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "Owning group property may "
          + "not be set through flowconfigs API, use flowconfigsV2");
    }

    List<ServiceRequester> requesterList = this.requesterService.findRequesters(this);

    try {
      String serialized = RequesterService.serialize(requesterList);
      flowConfig.getProperties().put(RequesterService.REQUESTER_LIST, serialized);
      LOG.info("Rest requester list is " + serialized);
    } catch (IOException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "cannot get who is the requester", e);
    }
    return this.flowConfigsResourceHandler.createFlowConfig(flowConfig);
  }

  /**
   * Update the flow configuration with the specified key. Running flows are not affected.
   * An error is raised if the flow configuration does not exist.
   * @param key composite key containing group name and flow name that identifies the flow to update
   * @param flowConfig new flow configuration
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse update(ComplexResourceKey<FlowId, EmptyRecord> key, FlowConfig flowConfig) {
    if (flowConfig.hasOwningGroup()) {
      throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "Owning group property may "
          + "not be set through flowconfigs API, use flowconfigsV2");
    }
    if (flowConfig.getProperties().containsKey(RequesterService.REQUESTER_LIST)) {
      throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, RequesterService.REQUESTER_LIST + " property may "
          + "not be set through flowconfigs API, use flowconfigsV2");
    }
    checkRequester(this.requesterService, get(key), this.requesterService.findRequesters(this));
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.flowConfigsResourceHandler.updateFlowConfig(flowId, flowConfig);
  }

  /**
   * Delete a configured flow. Running flows are not affected. The schedule will be removed for scheduled flows.
   * @param key composite key containing flow group and flow name that identifies the flow to remove from the flow catalog
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowId, EmptyRecord> key) {
    checkRequester(this.requesterService, get(key), this.requesterService.findRequesters(this));
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.flowConfigsResourceHandler.deleteFlowConfig(flowId, getHeaders());
  }

  /**
   * Check that all {@link ServiceRequester}s in this request are contained within the original service requester list
   * when the flow was submitted. If they are not, throw a {@link FlowConfigLoggedException} with {@link HttpStatus#S_401_UNAUTHORIZED}.
   * If there is a failure when deserializing the original requester list, throw a {@link FlowConfigLoggedException} with
   * {@link HttpStatus#S_400_BAD_REQUEST}.
   *
   * @param requesterService the {@link RequesterService} used to verify the requester
   * @param originalFlowConfig original flow config to find original requester
   * @param requesterList list of requesters for this request
   */
  public static void checkRequester(
      RequesterService requesterService, FlowConfig originalFlowConfig, List<ServiceRequester> requesterList) {
    if (requesterService.isRequesterWhitelisted(requesterList)) {
      return;
    }

    try {
      String serializedOriginalRequesterList = originalFlowConfig.getProperties().get(RequesterService.REQUESTER_LIST);
      if (serializedOriginalRequesterList != null) {
        List<ServiceRequester> originalRequesterList = RequesterService.deserialize(serializedOriginalRequesterList);
        if (!requesterService.isRequesterAllowed(originalRequesterList, requesterList)) {
          throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "Requester not allowed to make this request");
        }
      }
    } catch (IOException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "Failed to get original requester list", e);
    }
  }

  private Properties getHeaders() {
    Properties headerProperties = new Properties();
    for (Map.Entry<String, String> entry : getContext().getRequestHeaders().entrySet()) {
      if (ALLOWED_METADATA.contains(entry.getKey())) {
        headerProperties.put(entry.getKey(), entry.getValue());
      }
    }
    return headerProperties;
  }
}

