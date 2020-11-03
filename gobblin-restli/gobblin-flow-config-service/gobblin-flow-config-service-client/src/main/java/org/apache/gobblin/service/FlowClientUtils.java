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

import java.net.URI;
import java.net.URISyntaxException;

import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;


/**
 * Utils to be used by clients
 */
public class FlowClientUtils {
  /**
   * Send a restli {@link Request} to the server through a {@link RestClient}, but if the request is rejected due to not
   * being sent to a leader node, get the leader node from the errorDetails and retry the request with that node by setting
   * the D2-Hint-TargetService attribute.
   * @param restClient rest client to use to send the request
   * @param request request to send
   * @param primaryResource resource part of the request URL (e.g. flowconfigsV2, which can be taken from
   *        {@link FlowconfigsV2RequestBuilders#getPrimaryResource()}
   * @return {@link Response} returned from the request
   * @throws RemoteInvocationException
   */
  public static Response<?> sendRequestWithRetry(RestClient restClient, Request<?> request, String primaryResource) throws RemoteInvocationException {
    Response<?> response;
    try {
      response = restClient.sendRequest(request).getResponse();
    } catch (RestLiResponseException exception) {
      if (exception.hasErrorDetails() && exception.getErrorDetails().containsKey(ServiceConfigKeys.LEADER_URL)) {
        String leaderUrl = exception.getErrorDetails().getString(ServiceConfigKeys.LEADER_URL);
        RequestContext requestContext = new RequestContext();
        try {
          requestContext.putLocalAttr("D2-Hint-TargetService", new URI(leaderUrl + "/" + primaryResource));
        } catch (URISyntaxException e) {
          throw new RuntimeException("Could not build URI for for url " + leaderUrl, e);
        }
        response = restClient.sendRequest(request, requestContext).getResponse();
      } else {
        throw exception;
      }
    }
    return response;
  }
}