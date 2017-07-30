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

package gobblin.util.limiter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.inject.Injector;
import com.linkedin.restli.server.resources.BaseResource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gobblin.broker.BrokerConfigurationKeyGenerator;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.SimpleScopeType;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.restli.EmbeddedRestliServer;
import gobblin.restli.throttling.CountBasedPolicy;
import gobblin.restli.throttling.LimiterServerResource;
import gobblin.restli.throttling.ThrottlingGuiceServletConfig;
import gobblin.restli.throttling.ThrottlingPolicyFactory;
import gobblin.util.limiter.broker.SharedLimiterKey;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.curator.test.TestingServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import lombok.Data;


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

    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();
    guiceServletConfig.initialize(ConfigFactory.parseMap(configMap));
    Injector injector = guiceServletConfig.getInjector();

    EmbeddedRestliServer server = EmbeddedRestliServer.builder().resources(
        Lists.<Class<? extends BaseResource>>newArrayList(LimiterServerResource.class)).injector(injector).build();

    SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    try {

      server.startAsync();
      server.awaitRunning();

      RestliServiceBasedLimiter limiter = RestliServiceBasedLimiter.builder()
          .requestSender(new RedirectAwareRestClientRequestSender(broker, Lists.newArrayList(server.getURIPrefix())))
          .resourceLimited(res1key.getResourceLimitedPath()).serviceIdentifier("service").build();

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

  @Test
  public void testServerFailover() throws Exception {
    try (Closer closer = Closer.create()) {
      SharedLimiterKey res1key = new SharedLimiterKey("res1");

      Map<String, String> configMap = Maps.newHashMap();

      TestingServer zkTestingServer = closer.register(new TestingServer(-1));
      configMap.put(ThrottlingGuiceServletConfig.ZK_STRING_KEY, zkTestingServer.getConnectString());
      configMap.put(ThrottlingGuiceServletConfig.HA_CLUSTER_NAME, RestliServiceBasedLimiterTest.class.getSimpleName() + "_cluster");

      Config config = ConfigFactory.parseMap(configMap);

      RestliServer server2500 = createAndStartServer(config, 2500);
      RestliServer server2501 = createAndStartServer(config, 2501);

      SharedResourcesBroker<SimpleScopeType> broker =
          SharedResourcesBrokerFactory.createDefaultTopLevelBroker(ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());


      RedirectAwareRestClientRequestSender requestSender = new RedirectAwareRestClientRequestSender(broker,
          Lists.newArrayList(server2500.getServer().getURIPrefix(), server2501.getServer().getURIPrefix()));
      RestliServiceBasedLimiter limiter = RestliServiceBasedLimiter.builder()
          .requestSender(requestSender)
          .resourceLimited(res1key.getResourceLimitedPath())
          .serviceIdentifier("service")
          .build();

      Assert.assertNotNull(limiter.acquirePermits(20));
      limiter.clearAllStoredPermits();

      server2500.close();
      Assert.assertNotNull(limiter.acquirePermits(20));
      Assert.assertEquals(parsePortOfCurrentServerPrefix(requestSender), 2501);
      limiter.clearAllStoredPermits();

      server2500 = createAndStartServer(config, 2500);
      Assert.assertNotNull(limiter.acquirePermits(20));
      limiter.clearAllStoredPermits();

      // leader is currently 2501
      Assert.assertEquals(parsePortOfCurrentServerPrefix(requestSender), 2501);
      // set request to 2500 (not leader)
      requestSender.updateRestClient(server2500.getServer().getURIPrefix(), "test");
      Assert.assertEquals(parsePortOfCurrentServerPrefix(requestSender), 2500);
      Assert.assertNotNull(limiter.acquirePermits(20));
      // verify request sender switched back to leader
      Assert.assertEquals(parsePortOfCurrentServerPrefix(requestSender), 2501);

      server2501.close();
      Assert.assertNotNull(limiter.acquirePermits(20));
      limiter.clearAllStoredPermits();

      server2500.close();
      Assert.assertNull(limiter.acquirePermits(20));
      limiter.clearAllStoredPermits();

    }
  }

  private int parsePortOfCurrentServerPrefix(RedirectAwareRestClientRequestSender requestSender) throws
                                                                                                 URISyntaxException{
    return new URI(requestSender.getCurrentServerPrefix()).getPort();
  }

  private RestliServer createAndStartServer(Config baseConfig, int port) {
    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();
    guiceServletConfig.initialize(baseConfig.withFallback(ConfigFactory.parseMap(ImmutableMap.of(
        ThrottlingGuiceServletConfig.LISTENING_PORT, Integer.toString(port)
    ))));
    Injector injector = guiceServletConfig.getInjector();

    EmbeddedRestliServer server = EmbeddedRestliServer.builder()
        .resources(Lists.<Class<? extends BaseResource>>newArrayList(LimiterServerResource.class))
        .injector(injector)
        .port(port)
        .build();
    server.startAsync();
    server.awaitRunning();

    return new RestliServer(server, guiceServletConfig);
  }

  @Data
  private static class RestliServer {
    private final EmbeddedRestliServer server;
    private final ThrottlingGuiceServletConfig guiceServletConfig;

    public void close() {
      this.server.stopAsync();
      this.server.awaitTerminated();
      this.guiceServletConfig.close();
    }
  }

}
