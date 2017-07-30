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

import com.google.inject.Injector;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.BrokerConfigurationKeyGenerator;
import gobblin.util.limiter.broker.SharedLimiterKey;


public class PoliciesResourceTest {

  @Test
  public void test() {
    ThrottlingGuiceServletConfig guiceServletConfig = new ThrottlingGuiceServletConfig();

    ThrottlingPolicyFactory factory = new ThrottlingPolicyFactory();
    SharedLimiterKey res1key = new SharedLimiterKey("res1");

    Map<String, String> configMap = avro.shaded.com.google.common.collect.ImmutableMap.<String, String>builder()
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, ThrottlingPolicyFactory.POLICY_KEY),
            CountBasedPolicy.FACTORY_ALIAS)
        .put(BrokerConfigurationKeyGenerator.generateKey(factory, res1key, null, CountBasedPolicy.COUNT_KEY), "100")
        .build();

    guiceServletConfig.initialize(ConfigFactory.parseMap(configMap));
    Injector injector = guiceServletConfig.getInjector();

    PoliciesResource policiesResource = injector.getInstance(PoliciesResource.class);

    Policy policy = policiesResource.get("res1");

    Assert.assertEquals(policy.getPolicyName(), CountBasedPolicy.class.getSimpleName());
    Assert.assertEquals(policy.getResource(), "res1");
    Assert.assertEquals(policy.getParameters().get("maxPermits"), "100");

    guiceServletConfig.close();
  }

}
