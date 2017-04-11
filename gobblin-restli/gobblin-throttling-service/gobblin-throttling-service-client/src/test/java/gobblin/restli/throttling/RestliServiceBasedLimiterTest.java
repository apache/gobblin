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

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;
import com.linkedin.r2.transport.http.client.HttpClientFactory;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.ConfigFactory;
import gobblin.broker.BrokerConfigurationKeyGenerator;
import gobblin.restli.EmbeddedRestliServer;
import gobblin.util.limiter.CountBasedLimiter;
import gobblin.util.limiter.RestliServiceBasedLimiter;
import gobblin.util.limiter.broker.SharedLimiterFactory;
import gobblin.util.limiter.broker.SharedLimiterKey;
import java.util.Collections;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RestliServiceBasedLimiterTest {

  @Test
  public void test() throws Exception {
    ThrottlingPolicyFactory factory = new ThrottlingPolicyFactory();
    SharedLimiterKey res1key = new SharedLimiterKey("res1");

    Map<String, String> configMap = ImmutableMap.<String, String>builder()
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, ThrottlingPolicyFactory.POLICY_KEY),
            CountBasedPolicy.FACTORY_ALIAS)
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, CountBasedPolicy.COUNT_KEY), "100")
        .build();

    Injector injector = ThrottlingGuiceServletConfig.getInjector(ConfigFactory.parseMap(configMap));

    EmbeddedRestliServer server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(LimiterServerResource.class)).injector(injector).build();

    try {

      server.startAsync();
      server.awaitRunning();

      final HttpClientFactory http = new HttpClientFactory();
      final Client r2Client = new TransportClientAdapter(http.getClient(Collections.<String, String>emptyMap()));

      RestClient restClient = new RestClient(r2Client, server.getURIPrefix());

      RestliServiceBasedLimiter limiter = RestliServiceBasedLimiter.builder().restClient(restClient)
          .resourceLimited(res1key.getResourceLimited()).serviceIdentifier("service").build();

      Assert.assertNotNull(limiter.acquirePermits(20));
      Assert.assertNotNull(limiter.acquirePermits(20));
      Assert.assertNull(limiter.acquirePermits(1000));
    } finally {
      if (server.isRunning()) {
        server.stopAsync();
        server.awaitTerminated();
      }
    }
  }

}
