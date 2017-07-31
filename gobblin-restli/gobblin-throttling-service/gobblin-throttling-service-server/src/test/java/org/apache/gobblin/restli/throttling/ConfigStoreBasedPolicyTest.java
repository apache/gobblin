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

import java.net.URL;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;
import org.apache.gobblin.util.limiter.broker.SharedLimiterKey;

import avro.shaded.com.google.common.collect.ImmutableMap;


public class ConfigStoreBasedPolicyTest {

  @Test
  public void test() throws Exception {

    URL prefix = getClass().getResource("/configStore");

    Config config = ConfigFactory.parseMap(ImmutableMap.of(
       ConfigClientBasedPolicyFactory.CONFIG_KEY_URI_PREFIX_KEY, "simple-" + prefix.toString()
    ));

    SharedResourcesBroker<ThrottlingServerScopes> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
        ConfigFactory.empty(), ThrottlingServerScopes.GLOBAL.defaultScopeInstance());

    ConfigClientBasedPolicyFactory policyFactory = new ConfigClientBasedPolicyFactory();

    ThrottlingPolicy policy =
        policyFactory.createPolicy(new SharedLimiterKey("ConfigBasedPolicyTest/resource1"), broker, config);
    Assert.assertEquals(policy.getClass(), QPSPolicy.class);
    Assert.assertEquals(((QPSPolicy) policy).getQps(), 100);

    policy =
        policyFactory.createPolicy(new SharedLimiterKey("ConfigBasedPolicyTest/resource2"), broker, config);
    Assert.assertEquals(policy.getClass(), CountBasedPolicy.class);
    Assert.assertEquals(((CountBasedPolicy) policy).getCount(), 50);
  }

}
