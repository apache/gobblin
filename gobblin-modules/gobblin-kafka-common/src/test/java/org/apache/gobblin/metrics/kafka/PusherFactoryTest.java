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

package org.apache.gobblin.metrics.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.StringNameSharedResourceKey;
import org.apache.gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import org.apache.gobblin.broker.gobblin_scopes.JobScopeInstance;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


/**
 * Test {@link PusherFactory}s
 */
public class PusherFactoryTest {

  @Test
  private void testCreateGobblinScopedDefaultPusher()
      throws NotConfiguredException {
    SharedResourcesBroker<GobblinScopeTypes> instanceBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(ConfigFactory.empty(), GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBroker<GobblinScopeTypes> jobBroker = instanceBroker.newSubscopedBuilder(
        new JobScopeInstance("PusherFactoryTest", String.valueOf(System.currentTimeMillis()))).build();

    StringNameSharedResourceKey key = new StringNameSharedResourceKey("test");

    Pusher<Object> pusher = jobBroker.getSharedResource(new GobblinScopePusherFactory<>(), key);
    Assert.assertEquals(pusher.getClass(), LoggingPusher.class);

    try {
      jobBroker.close();
      instanceBroker.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  private void testCreateGobblinScopedCustomPusher()
      throws NotConfiguredException {
    Map<String, String> configAsMap = new HashMap<>();
    configAsMap.put("gobblin.broker.pusher.class", TestPusher.class.getName());
    configAsMap.put("gobblin.broker.pusher.id", "sharedId");
    configAsMap.put("gobblin.broker.pusher.testPusher.id", "testPusherId");
    configAsMap.put("gobblin.broker.pusher.testPusher.name", "testPusherName");

    SharedResourcesBroker<GobblinScopeTypes> instanceBroker = SharedResourcesBrokerFactory
        .createDefaultTopLevelBroker(ConfigFactory.parseMap(configAsMap), GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBroker<GobblinScopeTypes> jobBroker = instanceBroker.newSubscopedBuilder(
        new JobScopeInstance("PusherFactoryTest", String.valueOf(System.currentTimeMillis()))).build();

    StringNameSharedResourceKey key = new StringNameSharedResourceKey("testPusher");

    Pusher<String> pusher = jobBroker.getSharedResource(new GobblinScopePusherFactory<>(), key);

    Assert.assertEquals(pusher.getClass(), TestPusher.class);
    TestPusher testPusher = (TestPusher) pusher;
    Assert.assertTrue(!testPusher.isClosed);
    Assert.assertEquals(testPusher.id, "testPusherId");
    Assert.assertEquals(testPusher.name, "testPusherName");

    try {
      jobBroker.close();
      instanceBroker.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Assert.assertTrue(testPusher.isClosed);
  }

  public static class TestPusher implements Pusher<String> {
    private boolean isClosed = false;
    private final String id;
    private final String name;

    public TestPusher(Config config) {
      id = config.getString("id");
      name = config.getString("name");
    }

    @Override
    public void pushMessages(List<String> messages) {
    }

    @Override
    public void close()
        throws IOException {
      isClosed = true;
    }
  }
}
