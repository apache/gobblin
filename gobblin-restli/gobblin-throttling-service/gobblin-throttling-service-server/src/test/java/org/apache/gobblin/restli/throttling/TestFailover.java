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

import java.net.URI;
import java.util.Map;

import org.apache.curator.test.TestingServer;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.RestLiServiceException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public class TestFailover {

  @Test
  public void test() throws Exception {

    try(Closer closer = Closer.create()) {
      Map<String, String> configMap = Maps.newHashMap();

      TestingServer zkTestingServer = closer.register(new TestingServer(-1));
      configMap.put(ThrottlingGuiceServletConfig.ZK_STRING_KEY, zkTestingServer.getConnectString());
      configMap.put(ThrottlingGuiceServletConfig.HA_CLUSTER_NAME, TestFailover.class.getSimpleName() + "_cluster");
      Config config = ConfigFactory.parseMap(configMap);

      ThrottlingGuiceServletConfig server2001 = createServerAtPort(config, 2001);

      PermitAllocation allocation = sendRequestToServer(server2001, 10);
      Assert.assertTrue(allocation.getPermits() >= 1);

      ThrottlingGuiceServletConfig server2002 = createServerAtPort(config, 2002);

      allocation = sendRequestToServer(server2001, 10);
      Assert.assertTrue(allocation.getPermits() >= 1);

      try {
        sendRequestToServer(server2002, 10);
        Assert.fail();
      } catch (RestLiServiceException exc) {
        Assert.assertTrue(exc.hasErrorDetails());
        Assert.assertTrue(exc.getErrorDetails().containsKey(LimiterServerResource.LOCATION_301));
        Assert.assertEquals(new URI(exc.getErrorDetails().get(LimiterServerResource.LOCATION_301).toString()).getPort(), 2001);
      }

      server2001.close();

      allocation = sendRequestToServer(server2002, 10);
      Assert.assertTrue(allocation.getPermits() >= 1);

    }

  }

  private ThrottlingGuiceServletConfig createServerAtPort(Config baseConfig, int port) {
    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();
    guiceServletConfig.initialize(baseConfig.withFallback(ConfigFactory.parseMap(
        ImmutableMap.of(ThrottlingGuiceServletConfig.LISTENING_PORT, port))));
    return guiceServletConfig;
  }

  private PermitAllocation sendRequestToServer(ThrottlingGuiceServletConfig guiceServletConfig, long permits) {
    return guiceServletConfig.getLimiterResource()
        .getSync(new ComplexResourceKey<>(createPermitRequest(10), new EmptyRecord()));
  }

  private PermitRequest createPermitRequest(long permits) {
    PermitRequest request = new PermitRequest();
    request.setPermits(permits);
    request.setRequestorIdentifier("requestor");
    request.setResource("test");
    return request;
  }

}
