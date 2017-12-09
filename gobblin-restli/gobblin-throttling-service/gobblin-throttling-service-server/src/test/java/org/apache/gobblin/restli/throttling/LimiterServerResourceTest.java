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

package org.apache.gobblin.restli.throttling;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Timer;
import com.google.inject.Injector;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.BrokerConfigurationKeyGenerator;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.util.limiter.broker.SharedLimiterKey;

import avro.shaded.com.google.common.collect.ImmutableMap;


public class LimiterServerResourceTest {

  @Test
  public void test() {
    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();
    guiceServletConfig.initialize(ConfigFactory.empty());
    Injector injector = guiceServletConfig.getInjector();

    LimiterServerResource limiterServer = injector.getInstance(LimiterServerResource.class);

    PermitRequest request = new PermitRequest();
    request.setPermits(10);
    request.setResource("myResource");
    PermitAllocation allocation = limiterServer.getSync(new ComplexResourceKey<>(request, new EmptyRecord()));

    Assert.assertTrue(allocation.getPermits() >= 10);

  }

  @Test
  public void testLimitedRequests() {

    ThrottlingPolicyFactory factory = new ThrottlingPolicyFactory();
    SharedLimiterKey res1key = new SharedLimiterKey("res1");
    SharedLimiterKey res2key = new SharedLimiterKey("res2");

    Map<String, String> configMap = ImmutableMap.<String, String>builder()
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, ThrottlingPolicyFactory.POLICY_KEY),
            CountBasedPolicy.FACTORY_ALIAS)
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, CountBasedPolicy.COUNT_KEY), "100")
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res2key, null, ThrottlingPolicyFactory.POLICY_KEY),
            CountBasedPolicy.FACTORY_ALIAS)
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res2key, null, CountBasedPolicy.COUNT_KEY), "50")
        .build();

    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();
    guiceServletConfig.initialize(ConfigFactory.parseMap(configMap));
    Injector injector = guiceServletConfig.getInjector();

    LimiterServerResource limiterServer = injector.getInstance(LimiterServerResource.class);

    PermitRequest res1request = new PermitRequest();
    res1request.setPermits(20);
    res1request.setResource(res1key.getResourceLimitedPath());

    PermitRequest res2request = new PermitRequest();
    res2request.setPermits(20);
    res2request.setResource(res2key.getResourceLimitedPath());

    PermitRequest res3request = new PermitRequest();
    res3request.setPermits(100000);
    res3request.setResource("res3");

    Assert.assertEquals(limiterServer.getSync(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.getSync(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.getSync(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.getSync(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.getSync(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));

    try {
      // out of permits
      limiterServer.getSync(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits();
      Assert.fail();
    } catch (RestLiServiceException exc) {
      Assert.assertEquals(exc.getStatus(), HttpStatus.S_403_FORBIDDEN);
    }

    Assert.assertEquals(limiterServer.getSync(new ComplexResourceKey<>(res2request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.getSync(new ComplexResourceKey<>(res2request, new EmptyRecord())).getPermits(), new Long(20));
    // out of permits
    try {
      // out of permits
      limiterServer.getSync(new ComplexResourceKey<>(res2request, new EmptyRecord())).getPermits();
      Assert.fail();
    } catch (RestLiServiceException exc) {
      Assert.assertEquals(exc.getStatus(), HttpStatus.S_403_FORBIDDEN);
    }

    // No limit
    Assert.assertTrue(limiterServer.getSync(new ComplexResourceKey<>(res3request, new EmptyRecord())).getPermits() >= res3request.getPermits());
  }

  @Test
  public void testMetrics() throws Exception {
    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();
    guiceServletConfig.initialize(ConfigFactory.empty());
    Injector injector = guiceServletConfig.getInjector();

    LimiterServerResource limiterServer = injector.getInstance(LimiterServerResource.class);

    PermitRequest request = new PermitRequest();
    request.setPermits(10);
    request.setResource("myResource");

    limiterServer.getSync(new ComplexResourceKey<>(request, new EmptyRecord()));
    limiterServer.getSync(new ComplexResourceKey<>(request, new EmptyRecord()));
    limiterServer.getSync(new ComplexResourceKey<>(request, new EmptyRecord()));

    MetricContext metricContext = limiterServer.metricContext;
    Timer timer = metricContext.timer(LimiterServerResource.REQUEST_TIMER_NAME);

    Assert.assertEquals(timer.getCount(), 3);
  }

}
