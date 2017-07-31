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

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.linkedin.common.callback.Callback;
import com.linkedin.restli.client.Response;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.BrokerConfigurationKeyGenerator;
import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SimpleScopeType;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.restli.SharedRestClientKey;
import org.apache.gobblin.restli.throttling.PermitAllocation;
import org.apache.gobblin.restli.throttling.PermitRequest;
import org.apache.gobblin.util.limiter.broker.SharedLimiterFactory;
import org.apache.gobblin.util.limiter.broker.SharedLimiterKey;


public class RestliLimiterFactoryTest {

  @Test
  public void testFactory() throws Exception {
    SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    MyRequestSender requestSender = new MyRequestSender();

    broker.bindSharedResourceAtScope(new RedirectAwareRestClientRequestSender.Factory<>(),
        new SharedRestClientKey(RestliLimiterFactory.RESTLI_SERVICE_NAME), SimpleScopeType.GLOBAL, requestSender);
    RestliServiceBasedLimiter limiter =
        broker.getSharedResource(new RestliLimiterFactory<>(), new SharedLimiterKey("my/resource"));

    Assert.assertNotNull(limiter.acquirePermits(10));
    Assert.assertEquals(requestSender.requestList.size(), 1);

    broker.close();
  }

  @Test
  public void testRestliLimiterCalledByLimiterFactory() throws Exception {
    SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    MyRequestSender requestSender = new MyRequestSender();

    broker.bindSharedResourceAtScope(new RedirectAwareRestClientRequestSender.Factory<>(),
        new SharedRestClientKey(RestliLimiterFactory.RESTLI_SERVICE_NAME), SimpleScopeType.GLOBAL, requestSender);
    Limiter limiter =
        broker.getSharedResource(new SharedLimiterFactory<>(), new SharedLimiterKey("my/resource"));

    Assert.assertNotNull(limiter.acquirePermits(10));
    Assert.assertEquals(requestSender.requestList.size(), 1);

    broker.close();
  }

  @Test
  public void testSkipGlobalLimiterOnLimiterFactory() throws Exception {
    Map<String, String> configMap = ImmutableMap.of(
        BrokerConfigurationKeyGenerator.generateKey(new SharedLimiterFactory(), null, null, SharedLimiterFactory.SKIP_GLOBAL_LIMITER_KEY), "true"
    );

    SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.parseMap(configMap), SimpleScopeType.GLOBAL.defaultScopeInstance());

    MyRequestSender requestSender = new MyRequestSender();

    broker.bindSharedResourceAtScope(new RedirectAwareRestClientRequestSender.Factory<>(),
        new SharedRestClientKey(RestliLimiterFactory.RESTLI_SERVICE_NAME), SimpleScopeType.GLOBAL, requestSender);
    Limiter limiter =
        broker.getSharedResource(new SharedLimiterFactory<>(), new SharedLimiterKey("my/resource"));

    Assert.assertNotNull(limiter.acquirePermits(10));
    Assert.assertEquals(requestSender.requestList.size(), 0);

    broker.close();
  }

  public static class MyRequestSender implements RequestSender {
    List<PermitRequest> requestList = Lists.newArrayList();

    @Override
    public void sendRequest(PermitRequest request, Callback<Response<PermitAllocation>> callback) {
      this.requestList.add(request);

      PermitAllocation permitAllocation = new PermitAllocation();
      permitAllocation.setPermits(request.getPermits());
      permitAllocation.setExpiration(Long.MAX_VALUE);

      Response<PermitAllocation> response = Mockito.mock(Response.class);
      Mockito.when(response.getEntity()).thenReturn(permitAllocation);
      callback.onSuccess(response);
    }
  }

}
