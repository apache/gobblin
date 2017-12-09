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

package org.apache.gobblin.util.limiter;

import com.linkedin.common.callback.Callback;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;

import org.apache.gobblin.restli.throttling.PermitAllocation;
import org.apache.gobblin.restli.throttling.PermitRequest;
import org.apache.gobblin.restli.throttling.PermitsGetRequestBuilder;
import org.apache.gobblin.restli.throttling.PermitsRequestBuilders;

import lombok.AllArgsConstructor;


/**
 * Sends requests to a server using a {@link RestClient}. Subclasses can decorate the callback to intercept
 * certain response statuses.
 */
@AllArgsConstructor
public abstract class RestClientRequestSender implements RequestSender {
  @Override
  public void sendRequest(PermitRequest request, Callback<Response<PermitAllocation>> callback) {
    PermitsGetRequestBuilder getBuilder = new PermitsRequestBuilders().get();
    Request<PermitAllocation> fullRequest = getBuilder.id(new ComplexResourceKey<>(request, new EmptyRecord())).build();
    getRestClient().sendRequest(fullRequest, decorateCallback(request, callback));
  }

  /**
   * Decorate the callback to intercept some responses.
   */
  protected Callback<Response<PermitAllocation>> decorateCallback(PermitRequest request,
      Callback<Response<PermitAllocation>> callback) {
    return callback;
  }

  /**
   * @return The {@link RestClient} to use to send the request.
   */
  protected abstract RestClient getRestClient();
}
