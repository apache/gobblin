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

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.BrokerConfigurationKeyGenerator;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.SimpleScopeType;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.limiter.CountBasedLimiter;
import gobblin.util.limiter.broker.SharedLimiterFactory;
import gobblin.util.limiter.broker.SharedLimiterKey;

import avro.shaded.com.google.common.collect.ImmutableMap;


public class LimiterServerResourceTest {

  @Test
  public void test() {

    final SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    Injector injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(SharedResourcesBroker.class).annotatedWith(Names.named(LimiterServerResource.BROKER_INJECT_NAME)).toInstance(broker);
      }
    });

    LimiterServerResource limiterServer = injector.getInstance(LimiterServerResource.class);

    PermitRequest request = new PermitRequest();
    request.setPermits(10);
    request.setResource("myResource");
    PermitAllocation allocation = limiterServer.get(new ComplexResourceKey<>(request, new EmptyRecord()));

    Assert.assertEquals(allocation.getPermits(), new Long(10));

  }

  @Test
  public void testLimitedRequests() {

    SharedLimiterFactory factory = new SharedLimiterFactory();
    SharedLimiterKey res1key = new SharedLimiterKey("res1");
    SharedLimiterKey res2key = new SharedLimiterKey("res2");

    Map<String, String> configMap = ImmutableMap.<String, String>builder()
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, SharedLimiterFactory.LIMITER_CLASS_KEY),
            CountBasedLimiter.FACTORY_ALIAS)
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, CountBasedLimiter.Factory.COUNT_KEY), "100")
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res2key, null, SharedLimiterFactory.LIMITER_CLASS_KEY),
            CountBasedLimiter.FACTORY_ALIAS)
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res2key, null, CountBasedLimiter.Factory.COUNT_KEY), "50")
        .build();

    final SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.parseMap(configMap), SimpleScopeType.GLOBAL.defaultScopeInstance());

    Injector injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(SharedResourcesBroker.class).annotatedWith(Names.named(LimiterServerResource.BROKER_INJECT_NAME)).toInstance(broker);
      }
    });

    LimiterServerResource limiterServer = injector.getInstance(LimiterServerResource.class);

    PermitRequest res1request = new PermitRequest();
    res1request.setPermits(20);
    res1request.setResource(res1key.getResourceLimited());

    PermitRequest res2request = new PermitRequest();
    res2request.setPermits(20);
    res2request.setResource(res2key.getResourceLimited());

    PermitRequest res3request = new PermitRequest();
    res3request.setPermits(100000);
    res3request.setResource("res3");

    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(20));
    // out of permits
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res1request, new EmptyRecord())).getPermits(), new Long(0));

    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res2request, new EmptyRecord())).getPermits(), new Long(20));
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res2request, new EmptyRecord())).getPermits(), new Long(20));
    // out of permits
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res2request, new EmptyRecord())).getPermits(), new Long(0));

    // No limit
    Assert.assertEquals(limiterServer.get(new ComplexResourceKey<>(res3request, new EmptyRecord())).getPermits(), res3request.getPermits());
  }

}
