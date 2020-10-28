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

import com.linkedin.restli.client.DeleteRequest;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.FindRequest;
import com.linkedin.restli.client.GetRequest;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.CollectionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;


/**
 * Flow execution client for REST flow execution server
 */
public class FlowExecutionClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FlowExecutionClient.class);

  private Optional<HttpClientFactory> _httpClientFactory;
  private Optional<RestClient> _restClient;
  private final FlowexecutionsRequestBuilders _flowexecutionsRequestBuilders;

  /**
   * Construct a {@link FlowExecutionClient} to communicate with http flow execution server at URI serverUri
   * @param serverUri address and port of the REST server
   */
  public FlowExecutionClient(String serverUri) {
    LOG.debug("FlowExecutionClient with serverUri " + serverUri);

    _httpClientFactory = Optional.of(new HttpClientFactory());
    Client r2Client = new TransportClientAdapter(_httpClientFactory.get().getClient(Collections.<String, String>emptyMap()));
    _restClient = Optional.of(new RestClient(r2Client, serverUri));

    _flowexecutionsRequestBuilders = createRequestBuilders();
  }

  /**
   * Construct a {@link FlowExecutionClient} to communicate with http flow execution server at URI serverUri
   * @param restClient restClient to send restli request
   */
  public FlowExecutionClient(RestClient restClient) {
    LOG.debug("FlowExecutionClient with restClient " + restClient);

    _httpClientFactory = Optional.absent();
    _restClient = Optional.of(restClient);

    _flowexecutionsRequestBuilders = createRequestBuilders();
  }

  protected FlowexecutionsRequestBuilders createRequestBuilders() {
    return new FlowexecutionsRequestBuilders();
  }

  /**
   * Get a flow execution
   * @param flowStatusId identifier of flow execution to get
   * @return a {@link FlowExecution} with the flow execution
   * @throws RemoteInvocationException
   */
  public FlowExecution getFlowExecution(FlowStatusId flowStatusId)
      throws RemoteInvocationException {
    LOG.debug("getFlowExecution with groupName " + flowStatusId.getFlowGroup() + " flowName " +
        flowStatusId.getFlowName());

    GetRequest<FlowExecution> getRequest = _flowexecutionsRequestBuilders.get()
        .id(new ComplexResourceKey<>(flowStatusId, new EmptyRecord())).build();

    Response<FlowExecution> response =
        _restClient.get().sendRequest(getRequest).getResponse();
    return response.getEntity();
  }

  /**
   * Get the latest flow execution
   * @param flowId identifier of flow execution to get
   * @return a {@link FlowExecution}
   * @throws RemoteInvocationException
   */
  public FlowExecution getLatestFlowExecution(FlowId flowId)
      throws RemoteInvocationException {
    LOG.debug("getFlowExecution with groupName " + flowId.getFlowGroup() + " flowName " +
        flowId.getFlowName());

    FindRequest<FlowExecution> findRequest = _flowexecutionsRequestBuilders.findByLatestFlowExecution().flowIdParam(flowId).build();

    Response<CollectionResponse<FlowExecution>> response =
        _restClient.get().sendRequest(findRequest).getResponse();

    List<FlowExecution> flowExecutionList = response.getEntity().getElements();

    if (flowExecutionList.isEmpty()) {
      return null;
    } else {
      Preconditions.checkArgument(flowExecutionList.size() == 1);
      return flowExecutionList.get(0);
    }
  }

  public List<FlowExecution> getLatestFlowExecution(FlowId flowId, Integer count, String tag) throws RemoteInvocationException {
    return getLatestFlowExecution(flowId, count, tag, null);
  }

  /**
   * Get the latest k flow executions
   * @param flowId identifier of flow execution to get
   * @return a list of {@link FlowExecution}es corresponding to the latest <code>count</code> executions, containing only
   * jobStatuses that match the given tag. If <code>executionStatus</code> is not null, only flows with that status are
   * returned.
   * @throws RemoteInvocationException
   */
  public List<FlowExecution> getLatestFlowExecution(FlowId flowId, Integer count, String tag, String executionStatus)
      throws RemoteInvocationException {
    LOG.debug("getFlowExecution with groupName " + flowId.getFlowGroup() + " flowName " +
        flowId.getFlowName() + " count " + Integer.toString(count));

    FindRequest<FlowExecution> findRequest = _flowexecutionsRequestBuilders.findByLatestFlowExecution().flowIdParam(flowId).
        addReqParam("count", count, Integer.class).addParam("tag", tag, String.class).addParam("executionStatus", executionStatus, String.class).build();

    Response<CollectionResponse<FlowExecution>> response =
        _restClient.get().sendRequest(findRequest).getResponse();

    List<FlowExecution> flowExecutionList = response.getEntity().getElements();

    if (flowExecutionList.isEmpty()) {
      return null;
    } else {
      return flowExecutionList;
    }
  }

  /**
   * Kill the flow with given FlowStatusId
   * @param flowStatusId identifier of flow execution to kill
   * @throws RemoteInvocationException
   */
  public void deleteFlowExecution(FlowStatusId flowStatusId)
      throws RemoteInvocationException {
    LOG.debug("deleteFlowExecution with groupName " + flowStatusId.getFlowGroup() + " flowName " +
        flowStatusId.getFlowName() + " flowExecutionId " + flowStatusId.getFlowExecutionId());

    DeleteRequest<FlowExecution> deleteRequest = _flowexecutionsRequestBuilders.delete()
        .id(new ComplexResourceKey<>(flowStatusId, new EmptyRecord())).build();

    FlowClientUtils.sendRequestWithRetry(_restClient.get(), deleteRequest, FlowexecutionsRequestBuilders.getPrimaryResource());
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