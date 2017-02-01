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

package gobblin.service;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

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

  /**
   * Construct a {@link FlowConfigClient} to communicate with http flow config server at URI serverUri
   * @param serverUri address and port of the REST server
   */
  public FlowConfigClient(String serverUri) {
    LOG.debug("FlowConfigClient with serverUri " + serverUri);

    _httpClientFactory = Optional.of(new HttpClientFactory());
    Client r2Client = new TransportClientAdapter(_httpClientFactory.get().getClient(Collections.<String, String>emptyMap()));
    _restClient = Optional.of(new RestClient(r2Client, serverUri));

    _flowconfigsRequestBuilders = new FlowconfigsRequestBuilders();
  }

  /**
   * Create a flow configuration
   * @param flowConfig flow configuration attributes
   * @throws RemoteInvocationException
   */
  public void createFlowConfig(FlowConfig flowConfig)
      throws RemoteInvocationException {
    LOG.debug("createFlowConfig with groupName " + flowConfig.getFlowGroup() + " flowName " + flowConfig.getFlowName());

    CreateIdRequest<ComplexResourceKey<FlowConfigId, EmptyRecord>, FlowConfig> request =
        _flowconfigsRequestBuilders.create().input(flowConfig).build();
    ResponseFuture<IdResponse<ComplexResourceKey<FlowConfigId, EmptyRecord>>> flowConfigResponseFuture =
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
    LOG.debug("updateFlowConfig with groupName " + flowConfig.getFlowGroup() + " flowName " + flowConfig.getFlowName());

    FlowConfigId flowConfigId = new FlowConfigId().setFlowGroup(flowConfig.getFlowGroup())
        .setFlowName(flowConfig.getFlowName());

    UpdateRequest<FlowConfig> updateRequest =
        _flowconfigsRequestBuilders.update().id(new ComplexResourceKey<>(flowConfigId, new EmptyRecord()))
            .input(flowConfig).build();

    ResponseFuture<EmptyRecord> response = _restClient.get().sendRequest(updateRequest);

    response.getResponse();
  }

  /**
   * Get a flow configuration
   * @param flowConfigId identifier of flow configuration to get
   * @return a {@link FlowConfig} with the flow configuration
   * @throws RemoteInvocationException
   */
  public FlowConfig getFlowConfig(FlowConfigId flowConfigId)
      throws RemoteInvocationException {
    LOG.debug("getFlowConfig with groupName " + flowConfigId.getFlowGroup() + " flowName " +
        flowConfigId.getFlowName());

    GetRequest<FlowConfig> getRequest = _flowconfigsRequestBuilders.get()
        .id(new ComplexResourceKey<>(flowConfigId, new EmptyRecord())).build();

    Response<FlowConfig> response =
        _restClient.get().sendRequest(getRequest).getResponse();
    return response.getEntity();
  }

  /**
   * Delete a flow configuration
   * @param flowConfigId identifier of flow configuration to delete
   * @throws RemoteInvocationException
   */
  public void deleteFlowConfig(FlowConfigId flowConfigId)
      throws RemoteInvocationException {
    LOG.debug("deleteFlowConfig with groupName " + flowConfigId.getFlowGroup() + " flowName " +
        flowConfigId.getFlowName());

    DeleteRequest<FlowConfig> deleteRequest = _flowconfigsRequestBuilders.delete()
        .id(new ComplexResourceKey<>(flowConfigId, new EmptyRecord())).build();
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