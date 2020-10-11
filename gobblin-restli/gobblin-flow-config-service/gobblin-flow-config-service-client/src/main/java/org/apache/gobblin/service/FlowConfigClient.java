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
import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.DeleteRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.UpdateRequest;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.IdResponse;


/**
 * Flow Configuration client for REST flow configuration server
 */
public class FlowConfigClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FlowConfigClient.class);

  private Optional<HttpClientFactory> _httpClientFactory;
  private Optional<RestClient> _restClient;
  private final FlowconfigsRequestBuilders _flowconfigsRequestBuilders;
  public static final String DELETE_STATE_STORE_KEY = "delete.state.store";

  /**
   * Construct a {@link FlowConfigClient} to communicate with http flow config server at URI serverUri
   * @param serverUri address and port of the REST server
   */
  public FlowConfigClient(String serverUri) {
    this(serverUri, Collections.emptyMap());
  }

  public FlowConfigClient(String serverUri, Map<String, String> properties) {
    LOG.debug("FlowConfigClient with serverUri " + serverUri);

    _httpClientFactory = Optional.of(new HttpClientFactory());
    Client r2Client = new TransportClientAdapter(_httpClientFactory.get().getClient(properties));
    _restClient = Optional.of(new RestClient(r2Client, serverUri));

    _flowconfigsRequestBuilders = createRequestBuilders();
  }

  /**
   * Construct a {@link FlowConfigClient} to communicate with http flow config server at URI serverUri
   * @param restClient restClient to send restli request
   */
  public FlowConfigClient(RestClient restClient) {
    LOG.debug("FlowConfigClient with restClient " + restClient);

    _httpClientFactory = Optional.absent();
    _restClient = Optional.of(restClient);

    _flowconfigsRequestBuilders = createRequestBuilders();
  }

  // Clients using different service name can override this method
  // RequestBuilders decide the name of the service requests go to.
  protected FlowconfigsRequestBuilders createRequestBuilders() {
    return new FlowconfigsRequestBuilders();
  }

  /**
   * Create a flow configuration
   * @param flowConfig flow configuration attributes
   * @throws RemoteInvocationException
   */
  public void createFlowConfig(FlowConfig flowConfig)
      throws RemoteInvocationException {
    LOG.debug("createFlowConfig with groupName " + flowConfig.getId().getFlowGroup() + " flowName " +
        flowConfig.getId().getFlowName());

    CreateIdRequest<ComplexResourceKey<FlowId, EmptyRecord>, FlowConfig> request =
        _flowconfigsRequestBuilders.create().input(flowConfig).build();
    ResponseFuture<IdResponse<ComplexResourceKey<FlowId, EmptyRecord>>> flowConfigResponseFuture =
        _restClient.get().sendRequest(request);

    flowConfigResponseFuture.getResponse();
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
        _flowconfigsRequestBuilders.update().id(new ComplexResourceKey<>(flowId, new EmptyRecord()))
            .input(flowConfig).build();

    ResponseFuture<EmptyRecord> response = _restClient.get().sendRequest(updateRequest);

    response.getResponse();
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

    GetRequest<FlowConfig> getRequest = _flowconfigsRequestBuilders.get()
        .id(new ComplexResourceKey<>(flowId, new EmptyRecord())).build();

    Response<FlowConfig> response =
        _restClient.get().sendRequest(getRequest).getResponse();
    return response.getEntity();
  }

  /**
   * Delete a flow configuration
   * @param flowId identifier of flow configuration to delete
   * @throws RemoteInvocationException
   */
  public void deleteFlowConfig(FlowId flowId)
      throws RemoteInvocationException {
    LOG.debug("deleteFlowConfig with groupName " + flowId.getFlowGroup() + " flowName " +
        flowId.getFlowName());

    DeleteRequest<FlowConfig> deleteRequest = _flowconfigsRequestBuilders.delete()
        .id(new ComplexResourceKey<>(flowId, new EmptyRecord())).build();
    ResponseFuture<EmptyRecord> response = _restClient.get().sendRequest(deleteRequest);

    response.getResponse();
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

    DeleteRequest<FlowConfig> deleteRequest = _flowconfigsRequestBuilders.delete()
        .id(new ComplexResourceKey<>(flowId, new EmptyRecord())).setHeader(DELETE_STATE_STORE_KEY, Boolean.TRUE.toString()).build();
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