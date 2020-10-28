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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.CreateIdEntityRequest;
import com.linkedin.restli.client.DeleteRequest;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.PartialUpdateRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.UpdateRequest;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdEntityResponse;
import com.linkedin.restli.common.PatchRequest;


/**
 * Flow Configuration client for REST flow configuration server
 */
public class FlowConfigV2Client implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FlowConfigV2Client.class);

  private Optional<HttpClientFactory> _httpClientFactory;
  private Optional<RestClient> _restClient;
  private final FlowconfigsV2RequestBuilders _flowconfigsV2RequestBuilders;
  public static final String DELETE_STATE_STORE_KEY = "delete.state.store";
  private static final Pattern flowStatusIdParams = Pattern.compile(".*params:\\((?<flowStatusIdParams>.*?)\\)");

  /**
   * Construct a {@link FlowConfigV2Client} to communicate with http flow config server at URI serverUri
   * @param serverUri address and port of the REST server
   */
  public FlowConfigV2Client(String serverUri) {
    this(serverUri, Collections.emptyMap());
  }

  public FlowConfigV2Client(String serverUri, Map<String, String> properties) {
    LOG.debug("FlowConfigClient with serverUri " + serverUri);

    _httpClientFactory = Optional.of(new HttpClientFactory());
    Client r2Client = new TransportClientAdapter(_httpClientFactory.get().getClient(properties));
    _restClient = Optional.of(new RestClient(r2Client, serverUri));
    _flowconfigsV2RequestBuilders = createRequestBuilders();
  }

  /**
   * Construct a {@link FlowConfigV2Client} to communicate with http flow config server at URI serverUri
   * @param restClient restClient to send restli request
   */
  public FlowConfigV2Client(RestClient restClient) {
    LOG.debug("FlowConfigV2Client with restClient " + restClient);

    _httpClientFactory = Optional.absent();
    _restClient = Optional.of(restClient);
    _flowconfigsV2RequestBuilders = createRequestBuilders();
  }

  // Clients using different service name can override this method
  // RequestBuilders decide the name of the service requests go to.
  protected FlowconfigsV2RequestBuilders createRequestBuilders() {
    return new FlowconfigsV2RequestBuilders();
  }

  /**
   * Create a flow configuration
   * It differs from {@link FlowConfigClient} in a way that it returns FlowStatusId,
   * which can be used to find the FlowExecutionId
   * @param flowConfig FlowConfig to be used to create the flow
   * @return FlowStatusId
   * @throws RemoteInvocationException
   */
  public FlowStatusId createFlowConfig(FlowConfig flowConfig)
      throws RemoteInvocationException {
    LOG.debug("createFlowConfig with groupName " + flowConfig.getId().getFlowGroup() + " flowName " +
        flowConfig.getId().getFlowName());

    CreateIdEntityRequest<ComplexResourceKey<FlowId, FlowStatusId>, FlowConfig> request =
        _flowconfigsV2RequestBuilders.createAndGet().input(flowConfig).build();
    Response<?> response = FlowClientUtils.sendRequestWithRetry(_restClient.get(), request, FlowconfigsV2RequestBuilders.getPrimaryResource());

    return createFlowStatusId(response.getLocation().toString());
  }

  private FlowStatusId createFlowStatusId(String locationHeader) {
    Matcher matcher = flowStatusIdParams.matcher(locationHeader);
    matcher.find();
    String allFields = matcher.group("flowStatusIdParams");
    String[] flowStatusIdParams = allFields.split(",");
    Map<String, String> paramsMap = new HashMap<>();
    for (String flowStatusIdParam : flowStatusIdParams) {
      paramsMap.put(flowStatusIdParam.split(":")[0], flowStatusIdParam.split(":")[1]);
    }
    FlowStatusId flowStatusId = new FlowStatusId()
        .setFlowName(paramsMap.get("flowName"))
        .setFlowGroup(paramsMap.get("flowGroup"));
    if (paramsMap.containsKey("flowExecutionId")) {
      flowStatusId.setFlowExecutionId(Long.parseLong(paramsMap.get("flowExecutionId")));
    }
    return flowStatusId;
  }

  /**
   * Update a flow configuration
   * @param flowConfig flow configuration attributes
   * @throws RemoteInvocationException
   */
  public void updateFlowConfig(FlowConfig flowConfig)
      throws RemoteInvocationException {
    LOG.debug("updateFlowConfig with groupName " + flowConfig.getId().getFlowGroup() + " flowName " +
        flowConfig.getId().getFlowName());

    FlowId flowId = new FlowId().setFlowGroup(flowConfig.getId().getFlowGroup())
        .setFlowName(flowConfig.getId().getFlowName());

    UpdateRequest<FlowConfig> updateRequest =
        _flowconfigsV2RequestBuilders.update().id(new ComplexResourceKey<>(flowId, new FlowStatusId()))
            .input(flowConfig).build();

    FlowClientUtils.sendRequestWithRetry(_restClient.get(), updateRequest, FlowconfigsV2RequestBuilders.getPrimaryResource());
  }

  /**
   * Partially update a flow configuration
   * @param flowId flow ID to update
   * @param flowConfigPatch {@link PatchRequest} containing changes to the flowConfig
   * @throws RemoteInvocationException
   */
  public void partialUpdateFlowConfig(FlowId flowId, PatchRequest<FlowConfig> flowConfigPatch) throws RemoteInvocationException {
    LOG.debug("partialUpdateFlowConfig with groupName " + flowId.getFlowGroup() + " flowName " +
        flowId.getFlowName());

    PartialUpdateRequest<FlowConfig> partialUpdateRequest =
        _flowconfigsV2RequestBuilders.partialUpdate().id(new ComplexResourceKey<>(flowId, new FlowStatusId()))
            .input(flowConfigPatch).build();

    FlowClientUtils.sendRequestWithRetry(_restClient.get(), partialUpdateRequest, FlowconfigsV2RequestBuilders.getPrimaryResource());
  }

  /**
   * Get a flow configuration
   * @param flowId identifier of flow configuration to get
   * @return a {@link FlowConfig} with the flow configuration
   * @throws RemoteInvocationException
   */
  public FlowConfig getFlowConfig(FlowId flowId)
      throws RemoteInvocationException {
    LOG.debug("getFlowConfig with groupName " + flowId.getFlowGroup() + " flowName " + flowId.getFlowName());

    GetRequest<FlowConfig> getRequest = _flowconfigsV2RequestBuilders.get()
        .id(new ComplexResourceKey<>(flowId, new FlowStatusId())).build();

    Response<FlowConfig> response = _restClient.get().sendRequest(getRequest).getResponse();
    return response.getEntity();
  }

  /**
   * Get all {@link FlowConfig}s
   * @return all {@link FlowConfig}s
   * @throws RemoteInvocationException
   */
  public Collection<FlowConfig> getAllFlowConfigs() throws RemoteInvocationException {
    LOG.debug("getAllFlowConfigs called");

    GetAllRequest<FlowConfig> getRequest = _flowconfigsV2RequestBuilders.getAll().build();
    Response<CollectionResponse<FlowConfig>> response = _restClient.get().sendRequest(getRequest).getResponse();
    return response.getEntity().getElements();
  }

  /**
   * Get all {@link FlowConfig}s that matches the provided parameters. All the parameters are optional.
   * If a parameter is null, it is ignored. {@see FlowConfigV2Resource#getFilteredFlows}
   */
  public Collection<FlowConfig> getFlowConfigs(String flowGroup, String flowName, String templateUri, String userToProxy,
      String sourceIdentifier, String destinationIdentifier, String schedule, Boolean isRunImmediately, String owningGroup,
      String propertyFilter) throws RemoteInvocationException {
    LOG.debug("getAllFlowConfigs called");

    FindRequest<FlowConfig> getRequest = _flowconfigsV2RequestBuilders.findByFilterFlows()
        .flowGroupParam(flowGroup).flowNameParam(flowName).templateUriParam(templateUri).userToProxyParam(userToProxy)
        .sourceIdentifierParam(sourceIdentifier).destinationIdentifierParam(destinationIdentifier).scheduleParam(schedule)
        .isRunImmediatelyParam(isRunImmediately).owningGroupParam(owningGroup).propertyFilterParam(propertyFilter).build();

    Response<CollectionResponse<FlowConfig>> response = _restClient.get().sendRequest(getRequest).getResponse();

    return response.getEntity().getElements();
  }

  /**
   * Delete a flow configuration
   * @param flowId identifier of flow configuration to delete
   * @throws RemoteInvocationException
   */
  public void deleteFlowConfig(FlowId flowId)
      throws RemoteInvocationException {
    LOG.debug("deleteFlowConfig with groupName {}, flowName {}", flowId.getFlowGroup(), flowId.getFlowName());

    DeleteRequest<FlowConfig> deleteRequest = _flowconfigsV2RequestBuilders.delete()
        .id(new ComplexResourceKey<>(flowId, new FlowStatusId())).build();

    FlowClientUtils.sendRequestWithRetry(_restClient.get(), deleteRequest, FlowconfigsV2RequestBuilders.getPrimaryResource());
  }

  /**
   * Delete a flow configuration
   * @param flowId identifier of flow configuration to delete
   * @throws RemoteInvocationException
   */
  public void deleteFlowConfigWithStateStore(FlowId flowId)
      throws RemoteInvocationException {
    LOG.debug("deleteFlowConfig and state store with groupName " + flowId.getFlowGroup() + " flowName " +
        flowId.getFlowName());

    DeleteRequest<FlowConfig> deleteRequest = _flowconfigsV2RequestBuilders.delete()
        .id(new ComplexResourceKey<>(flowId, new FlowStatusId())).setHeader(DELETE_STATE_STORE_KEY, Boolean.TRUE.toString()).build();
    ResponseFuture<EmptyRecord> response = _restClient.get().sendRequest(deleteRequest);

    response.getResponse();
  }

  @Override
  public void close()
      throws IOException {
    if (_restClient.isPresent()) {
      _restClient.get().shutdown(new FutureCallback<None>());
    }

    if (_httpClientFactory.isPresent()) {
      _httpClientFactory.get().shutdown(new FutureCallback<None>());
    }
  }
}