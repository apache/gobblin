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

package gobblin.r2;

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.Facilities;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.transport.common.TransportClientFactory;


/**
 * The proxy takes care of {@link TransportClientFactory}s shutdown while shutting down {@link D2Client}
 */
public class D2ClientProxy implements D2Client {
  private D2Client d2Client;
  private Collection<TransportClientFactory> clientFactories;

  D2ClientProxy(D2Client d2Client, Collection<TransportClientFactory> clientFactories) {
    this.d2Client = d2Client;
    this.clientFactories = clientFactories;
  }

  @Override
  public Facilities getFacilities() {
    return d2Client.getFacilities();
  }

  @Override
  public void start(Callback<None> callback) {
    d2Client.start(callback);
  }

  @Override
  public Future<RestResponse> restRequest(RestRequest request) {
    return d2Client.restRequest(request);
  }

  @Override
  public Future<RestResponse> restRequest(RestRequest request, RequestContext requestContext) {
    return d2Client.restRequest(request, requestContext);
  }

  @Override
  public void restRequest(RestRequest request, Callback<RestResponse> callback) {
    d2Client.restRequest(request, callback);
  }

  @Override
  public void restRequest(RestRequest request, RequestContext requestContext, Callback<RestResponse> callback) {
    d2Client.restRequest(request, requestContext, callback);
  }

  @Override
  public void streamRequest(StreamRequest request, Callback<StreamResponse> callback) {
    d2Client.streamRequest(request, callback);
  }

  @Override
  public void streamRequest(StreamRequest request, RequestContext requestContext, Callback<StreamResponse> callback) {
    d2Client.streamRequest(request, requestContext, callback);
  }

  @Override
  public Map<String, Object> getMetadata(URI uri) {
    return d2Client.getMetadata(uri);
  }

  @Override
  public void shutdown(Callback<None> callback) {
    d2Client.shutdown(callback);
    for (TransportClientFactory clientFactory : clientFactories) {
      clientFactory.shutdown(new FutureCallback<>());
    }
  }
}
