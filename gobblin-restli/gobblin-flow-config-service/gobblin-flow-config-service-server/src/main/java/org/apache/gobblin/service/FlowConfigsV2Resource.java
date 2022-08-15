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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.DataMap;
import com.linkedin.data.transform.DataProcessingException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.common.PatchRequest;
import com.linkedin.restli.internal.server.util.DataMapUtils;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.CreateResponse;
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
import com.linkedin.restli.server.annotations.ReturnEntity;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;
import com.linkedin.restli.server.util.PatchApplier;

import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.FlowSpecSearchObject;


/**
 * Resource for handling flow configuration requests
 */
@Slf4j
@RestLiCollection(name = "flowconfigsV2", namespace = "org.apache.gobblin.service", keyName = "id")
public class FlowConfigsV2Resource extends ComplexKeyResourceTemplate<FlowId, FlowStatusId, FlowConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(FlowConfigsV2Resource.class);
  public static final String INJECT_READY_TO_USE = "v2ReadyToUse";
  private static final Set<String> ALLOWED_METADATA = ImmutableSet.of("delete.state.store");


  @edu.umd.cs.findbugs.annotations.SuppressWarnings("MS_SHOULD_BE_FINAL")
  public static FlowConfigsResourceHandler global_flowConfigsResourceHandler = null;

  @Inject
  private FlowConfigsV2ResourceHandler flowConfigsResourceHandler;

  // For getting who sends the request
  @Inject
  private RequesterService requesterService;

  // For blocking use of this resource until it is ready
  @Inject
  @Named(INJECT_READY_TO_USE)
  private Boolean readyToUse;

  @Inject
  private GroupOwnershipService groupOwnershipService;

  public FlowConfigsV2Resource() {
  }

  /**
   * Retrieve the flow configuration with the given key
   * @param key flow config id key containing group name and flow name
   * @return {@link FlowConfig} with flow configuration
   */
  @Override
  public FlowConfig get(ComplexResourceKey<FlowId, FlowStatusId> key) {
    return this.getFlowConfigResourceHandler().getFlowConfig(key.getKey());
  }

  /**
   * Retrieve all the flow configurations
   */
  @Override
  public List<FlowConfig> getAll(@Context PagingContext pagingContext) {
    // Check to see if the count and start parameters are user defined or default from the framework
    if (!pagingContext.hasCount() && !pagingContext.hasStart())
      return (List) this.getFlowConfigResourceHandler().getAllFlowConfigs();
    else {
      return (List) this.getFlowConfigResourceHandler().getAllFlowConfigs(pagingContext.getStart(), pagingContext.getCount());
    }
  }

  /**
   * Get all {@link FlowConfig}s that matches the provided parameters. All the parameters are optional.
   * If a parameter is null, it is ignored. {@see FlowConfigV2Resource#getFilteredFlows}
   */
  @Finder("filterFlows")
  public List<FlowConfig> getFilteredFlows(@Context PagingContext context,
      @Optional @QueryParam("flowGroup") String flowGroup,
      @Optional @QueryParam("flowName") String flowName,
      @Optional @QueryParam("templateUri") String templateUri,
      @Optional @QueryParam("userToProxy") String userToProxy,
      @Optional @QueryParam("sourceIdentifier") String sourceIdentifier,
      @Optional @QueryParam("destinationIdentifier") String destinationIdentifier,
      @Optional @QueryParam("schedule") String schedule,
      @Optional @QueryParam("isRunImmediately") Boolean isRunImmediately,
      @Optional @QueryParam("owningGroup") String owningGroup,
      @Optional @QueryParam("propertyFilter") String propertyFilter) {
    FlowSpecSearchObject flowSpecSearchObject;
    // Check to see if the count and start parameters are user defined or default from the framework
    // Start is the index of the first specStore configurations to return
    // Count is the total number of specStore configurations to return
    if (!context.hasCount() && !context.hasStart()){
      flowSpecSearchObject = new FlowSpecSearchObject(null, flowGroup, flowName,
          templateUri, userToProxy, sourceIdentifier, destinationIdentifier, schedule, null,
          isRunImmediately, owningGroup, propertyFilter, -1, -1);
    }
    else {
      flowSpecSearchObject = new FlowSpecSearchObject(null, flowGroup, flowName,
          templateUri, userToProxy, sourceIdentifier, destinationIdentifier, schedule, null,
          isRunImmediately, owningGroup, propertyFilter, context.getStart(), context.getCount());
    }

    return (List) this.getFlowConfigResourceHandler().getFlowConfig(flowSpecSearchObject);
  }

  /**
   * Create a flow configuration that the service will forward to execution instances for execution
   * @param flowConfig flow configuration
   * @return {@link CreateResponse}
   */
  @ReturnEntity
  @Override
  public CreateKVResponse create(FlowConfig flowConfig) {
    List<ServiceRequester> requesterList = this.requesterService.findRequesters(this);
    try {
      String serialized = RequesterService.serialize(requesterList);
      flowConfig.getProperties().put(RequesterService.REQUESTER_LIST, serialized);
      LOG.info("Rest requester list is " + serialized);
      if (flowConfig.hasOwningGroup() && !this.groupOwnershipService.isMemberOfGroup(requesterList, flowConfig.getOwningGroup())) {
        throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "Requester not part of owning group specified");
      }
    } catch (IOException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "cannot get who is the requester", e);
    }
    return (CreateKVResponse) this.getFlowConfigResourceHandler().createFlowConfig(flowConfig);
  }

  /**
   * Update the flow configuration with the specified key. Running flows are not affected.
   * An error is raised if the flow configuration does not exist.
   * @param key composite key containing group name and flow name that identifies the flow to update
   * @param flowConfig new flow configuration
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse update(ComplexResourceKey<FlowId, FlowStatusId> key, FlowConfig flowConfig) {
    checkUpdateDeleteAllowed(get(key), flowConfig);
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.getFlowConfigResourceHandler().updateFlowConfig(flowId, flowConfig);
  }

  /**
   * Partial update the flowConfig specified
   * @param key composite key containing group name and flow name that identifies the flow to update
   * @param flowConfigPatch patch describing what fields to change
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse update(ComplexResourceKey<FlowId, FlowStatusId> key, PatchRequest<FlowConfig> flowConfigPatch) {
    // Apply patch to an empty FlowConfig just to check which properties are being set
    FlowConfig flowConfig = new FlowConfig();
    try {
      PatchApplier.applyPatch(flowConfig, flowConfigPatch);
    } catch (DataProcessingException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "Failed to apply patch", e);
    }
    checkUpdateDeleteAllowed(get(key), flowConfig);
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.getFlowConfigResourceHandler().partialUpdateFlowConfig(flowId, flowConfigPatch);
  }

  /**
   * Delete a configured flow. Running flows are not affected. The schedule will be removed for scheduled flows.
   * @param key composite key containing flow group and flow name that identifies the flow to remove from the flow catalog
   * @return {@link UpdateResponse}
   */
  @Override
  public UpdateResponse delete(ComplexResourceKey<FlowId, FlowStatusId> key) {
    checkUpdateDeleteAllowed(get(key), null);
    String flowGroup = key.getKey().getFlowGroup();
    String flowName = key.getKey().getFlowName();
    FlowId flowId = new FlowId().setFlowGroup(flowGroup).setFlowName(flowName);
    return this.getFlowConfigResourceHandler().deleteFlowConfig(flowId, getHeaders());
  }

  /**
   * Trigger a new execution of an existing flow
   * @param pathKeys key of {@link FlowId} specified in path
   */
  @Action(name="runImmediately", resourceLevel=ResourceLevel.ENTITY)
  public String runImmediately(@PathKeysParam PathKeys pathKeys) {
    String patchJson = "{\"schedule\":{\"$set\":{\"runImmediately\":true}}}";
    DataMap dataMap = DataMapUtils.readMap(IOUtils.toInputStream(patchJson, Charset.defaultCharset()));
    PatchRequest<FlowConfig> flowConfigPatch = PatchRequest.createFromPatchDocument(dataMap);
    ComplexResourceKey<FlowId, FlowStatusId> id = pathKeys.get("id");
    update(id, flowConfigPatch);
    return "Successfully triggered flow " + id.getKey().toString();
  }

  private FlowConfigsResourceHandler getFlowConfigResourceHandler() {
    if (global_flowConfigsResourceHandler != null) {
      return global_flowConfigsResourceHandler;
    }
    return flowConfigsResourceHandler;
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

  /**
   * Check that this update or delete operation is allowed, throw a {@link FlowConfigLoggedException} if not.
   */
  public void checkUpdateDeleteAllowed(FlowConfig originalFlowConfig, FlowConfig updatedFlowConfig) {
    List<ServiceRequester> requesterList = this.requesterService.findRequesters(this);
    if (updatedFlowConfig != null) {
      checkPropertyUpdatesAllowed(requesterList, updatedFlowConfig);
    }
    checkRequester(originalFlowConfig, requesterList);
  }

  /**
   * Check that the properties being updated are allowed to be updated. This includes:
   * 1. Checking that the requester is part of the owningGroup if it is being modified
   * 2. Checking if the {@link RequesterService#REQUESTER_LIST} is being modified, and only allow it if a user is changing
   *    it to themselves.
   */
  public void checkPropertyUpdatesAllowed(List<ServiceRequester> requesterList, FlowConfig updatedFlowConfig) {
    if (this.requesterService.isRequesterWhitelisted(requesterList)) {
      return;
    }

    // Check that requester is part of owning group if owning group is being updated
    if (updatedFlowConfig.hasOwningGroup() && !this.groupOwnershipService.isMemberOfGroup(requesterList, updatedFlowConfig.getOwningGroup())) {
      throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "Requester not part of owning group specified. Requester " + requesterList
      + " should join group " + updatedFlowConfig.getOwningGroup() + " and retry.");
    }

    if (updatedFlowConfig.hasProperties() && updatedFlowConfig.getProperties().containsKey(RequesterService.REQUESTER_LIST)) {
      List<ServiceRequester> updatedRequesterList;
      try {
        updatedRequesterList = RequesterService.deserialize(updatedFlowConfig.getProperties().get(RequesterService.REQUESTER_LIST));
      } catch (Exception e) {
        String exampleRequester = "";
        try {
          List<ServiceRequester> exampleRequesterList = new ArrayList<>();
          exampleRequesterList.add(new ServiceRequester("name", "type", "from"));
          exampleRequester = " An example requester is " + RequesterService.serialize(exampleRequesterList);
        } catch (IOException ioe) {
          log.error("Failed to serialize example requester list", e);
        }
        throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, RequesterService.REQUESTER_LIST + " property was "
            + "provided but could not be deserialized." + exampleRequester, e);
      }

      if (!updatedRequesterList.equals(requesterList)) {
        throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, RequesterService.REQUESTER_LIST + " property may "
            + "only be updated to yourself. Requesting user: " + requesterList + ", updated requester: " + updatedRequesterList);
      }
    }
  }

  /**
   * Check that all {@link ServiceRequester}s in this request are contained within the original service requester list
   * or is part of the original requester's owning group when the flow was submitted. If they are not, throw a {@link FlowConfigLoggedException} with {@link HttpStatus#S_401_UNAUTHORIZED}.
   * If there is a failure when deserializing the original requester list, throw a {@link FlowConfigLoggedException} with
   * {@link HttpStatus#S_400_BAD_REQUEST}.
   * @param originalFlowConfig original flow config to find original requester
   * @param requesterList list of requesters for this request
   */
  public void checkRequester(FlowConfig originalFlowConfig, List<ServiceRequester> requesterList) {
    if (this.requesterService.isRequesterWhitelisted(requesterList)) {
      return;
    }

    try {
      String serializedOriginalRequesterList = originalFlowConfig.getProperties().get(RequesterService.REQUESTER_LIST);
      if (serializedOriginalRequesterList != null) {
        List<ServiceRequester> originalRequesterList = RequesterService.deserialize(serializedOriginalRequesterList);
        if (!requesterService.isRequesterAllowed(originalRequesterList, requesterList)) {
          // if the requester is not whitelisted or the original requester, reject the requester if it is not part of the owning group
          // of the original requester
          if (!(originalFlowConfig.hasOwningGroup() && this.groupOwnershipService.isMemberOfGroup(
              requesterList, originalFlowConfig.getOwningGroup()))) {
            throw new FlowConfigLoggedException(HttpStatus.S_401_UNAUTHORIZED, "Requester not allowed to make this request");
          }
        }
      }
    } catch (IOException e) {
      throw new FlowConfigLoggedException(HttpStatus.S_400_BAD_REQUEST, "Failed to get original requester list", e);
    }
  }
}
