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

package gobblin.restli.throttling;

import java.util.Collections;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.BrokerConfigurationKeyGenerator;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.SimpleScopeType;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.restli.EmbeddedRestliServer;
import gobblin.util.limiter.CountBasedLimiter;
import gobblin.util.limiter.broker.SharedLimiterFactory;
import gobblin.util.limiter.broker.SharedLimiterKey;

import avro.shaded.com.google.common.collect.ImmutableMap;


public class ThrottlingClientTest {

  @Test
  public void test() throws Exception {

    SharedLimiterFactory factory = new SharedLimiterFactory();
    SharedLimiterKey res1key = new SharedLimiterKey("res1");

    Map<String, String> configMap = ImmutableMap.<String, String>builder()
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, SharedLimiterFactory.LIMITER_CLASS_KEY),
            CountBasedLimiter.FACTORY_ALIAS)
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, CountBasedLimiter.Factory.COUNT_KEY), "50")
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, null, null, SharedLimiterFactory.FAIL_ON_UNKNOWN_RESOURCE_ID),
            "true")
        .build();
    final SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.parseMap(configMap), SimpleScopeType.GLOBAL.defaultScopeInstance());
    Injector injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(SharedResourcesBroker.class).annotatedWith(Names.named(LimiterServerResource.BROKER_INJECT_NAME)).toInstance(broker);
      }
    });

    EmbeddedRestliServer server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(LimiterServerResource.class)).injector(injector).build();

    try {

      server.startAsync();
      server.awaitRunning();

      final HttpClientFactory http = new HttpClientFactory();
      final Client r2Client = new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));

      RestClient restClient = new RestClient(r2Client, server.getURIPrefix());

      PermitsGetRequestBuilder getBuilder = new PermitsRequestBuilders().get();

      PermitRequest res1request = new PermitRequest();
      res1request.setPermits(20);
      res1request.setResource(res1key.getResourceLimited());

      PermitAllocation allocation = getPermitAllocation(res1request, restClient, getBuilder);
      Assert.assertEquals(allocation.getPermits(), new Long(20));

      allocation = getPermitAllocation(res1request, restClient, getBuilder);
      Assert.assertEquals(allocation.getPermits(), new Long(20));

      // out of permits
      allocation = getPermitAllocation(res1request, restClient, getBuilder);
      Assert.assertEquals(allocation.getPermits(), new Long(0));

      PermitRequest invalidRequest = new PermitRequest();
      invalidRequest.setPermits(20);
      invalidRequest.setResource("invalidkey");

      try {
        allocation = getPermitAllocation(invalidRequest, restClient, getBuilder);
        Assert.fail();
      } catch (RestLiResponseException exc) {
        Assert.assertEquals(exc.getStatus(), 422);
      }

    } finally {
      if (server.isRunning()) {
        server.stopAsync();
        server.awaitTerminated();
      }
    }

  }

  private PermitAllocation getPermitAllocation(PermitRequest permitRequest, RestClient restClient,
      PermitsGetRequestBuilder getBuilder) throws Exception {
    Request<PermitAllocation> request = getBuilder.id(new ComplexResourceKey<>(permitRequest, new EmptyRecord())).build();
    ResponseFuture<PermitAllocation> responseFuture = restClient.sendRequest(request);
    Response<PermitAllocation> response = responseFuture.getResponse();
    return response.getEntity();
  }

}
