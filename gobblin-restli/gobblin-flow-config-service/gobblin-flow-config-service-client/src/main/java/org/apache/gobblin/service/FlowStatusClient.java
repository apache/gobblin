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

import com.google.common.base.Preconditions;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.common.CollectionResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
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
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;


/**
 * Flow status client for REST flow status server
 */
public class FlowStatusClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FlowStatusClient.class);

  private Optional<HttpClientFactory> _httpClientFactory;
  private Optional<RestClient> _restClient;
  private final FlowstatusesRequestBuilders _flowstatusesRequestBuilders;

  /**
   * Construct a {@link FlowStatusClient} to communicate with http flow status server at URI serverUri
   * @param serverUri address and port of the REST server
   */
  public FlowStatusClient(String serverUri) {
    LOG.debug("FlowConfigClient with serverUri " + serverUri);

    _httpClientFactory = Optional.of(new HttpClientFactory());
    Client r2Client = new TransportClientAdapter(_httpClientFactory.get().getClient(Collections.<String, String>emptyMap()));
    _restClient = Optional.of(new RestClient(r2Client, serverUri));

    _flowstatusesRequestBuilders = new FlowstatusesRequestBuilders();
  }

  /**
   * Construct a {@link FlowStatusClient} to communicate with http flow status server at URI serverUri
   * @param restClient restClient to send restli request
   */
  public FlowStatusClient(RestClient restClient) {
    LOG.debug("FlowConfigClient with restClient " + restClient);

    _httpClientFactory = Optional.absent();
    _restClient = Optional.of(restClient);

    _flowstatusesRequestBuilders = new FlowstatusesRequestBuilders();
  }

  /**
   * Get a flow status
   * @param flowStatusId identifier of flow status to get
   * @return a {@link FlowStatus} with the flow status
   * @throws RemoteInvocationException
   */
  public FlowStatus getFlowStatus(FlowStatusId flowStatusId)
      throws RemoteInvocationException {
    LOG.debug("getFlowConfig with groupName " + flowStatusId.getFlowGroup() + " flowName " +
        flowStatusId.getFlowName());

    GetRequest<FlowStatus> getRequest = _flowstatusesRequestBuilders.get()
        .id(new ComplexResourceKey<>(flowStatusId, new EmptyRecord())).build();

    Response<FlowStatus> response =
        _restClient.get().sendRequest(getRequest).getResponse();
    return response.getEntity();
  }

  /**
   * Get the latest flow status
   * @param flowId identifier of flow status to get
   * @return a {@link FlowStatus} with the flow status
   * @throws RemoteInvocationException
   */
  public FlowStatus getLatestFlowStatus(FlowId flowId)
      throws RemoteInvocationException {
    LOG.debug("getFlowConfig with groupName " + flowId.getFlowGroup() + " flowName " +
        flowId.getFlowName());

    FindRequest<FlowStatus> findRequest = _flowstatusesRequestBuilders.findByLatestFlowStatus().flowIdParam(flowId).build();

    Response<CollectionResponse<FlowStatus>> response =
        _restClient.get().sendRequest(findRequest).getResponse();

    List<FlowStatus> flowStatusList = response.getEntity().getElements();

    if (flowStatusList.isEmpty()) {
      return null;
    } else {
      Preconditions.checkArgument(flowStatusList.size() == 1);
      return flowStatusList.get(0);
    }
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