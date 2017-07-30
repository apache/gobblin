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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.SettableFuture;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.d2.balancer.Facilities;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.message.stream.StreamRequest;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.transport.common.TransportClientFactory;
import com.linkedin.r2.transport.http.client.HttpClientFactory;


/**
 * The proxy takes care of {@link TransportClientFactory}s shutdown
 */
public class D2ClientProxy implements D2Client {
  private final D2Client d2Client;
  private final Collection<TransportClientFactory> clientFactories;

  D2ClientProxy(D2ClientBuilder builder, boolean isSSLEnabled) {
    if (isSSLEnabled) {
      Map<String, TransportClientFactory> factoryMap = createTransportClientFactories();
      builder.setClientFactories(factoryMap);
      clientFactories = factoryMap.values();
    } else {
      clientFactories = new ArrayList<>();
    }

    d2Client = buildClient(builder);
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

  private D2Client buildClient(D2ClientBuilder builder) {
    D2Client d2 = builder.build();
    final SettableFuture<None> d2ClientFuture = SettableFuture.create();
    d2.start(new Callback<None>() {
      @Override
      public void onError(Throwable e) {
        d2ClientFuture.setException(e);
      }
      @Override
      public void onSuccess(None none) {
        d2ClientFuture.set(none);
      }
    });

    try {
      // Synchronously wait for d2 to start
      d2ClientFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return d2;
  }

  private static Map<String, TransportClientFactory> createTransportClientFactories() {
    return ImmutableMap.<String, TransportClientFactory>builder()
        .put("http", new HttpClientFactory())
        //It won't route to SSL port without this.
        .put("https", new HttpClientFactory())
        .build();
  }
}
