/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.broker;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.iface.NoSuchScopeException;


public class DefaultGobblinBrokerTest {

  private static final Joiner JOINER = Joiner.on(".");

  @Test
  public void testSharedObjects() throws Exception {
    // Correct creation behavior
    Config config = ConfigFactory.empty();

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();
    SharedResourcesBrokerImpl<GobblinScopes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopes.CONTAINER, "thisContainer").build();
    SharedResourcesBrokerImpl<GobblinScopes> taskBroker = jobBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskabc")
        .withAdditionalParentBroker(containerBroker).build();
    SharedResourcesBrokerImpl<GobblinScopes> taskBroker2 = jobBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskxyz")
        .withAdditionalParentBroker(containerBroker).build();

    // create a shared resource
    TestFactory.SharedResource resource =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.JOB);

    Assert.assertEquals(resource.getKey(), "myKey");

    // using same broker with same scope and key returns same object
    Assert.assertEquals(taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.JOB),
        resource);
    // using different broker with same scope and key returns same object
    Assert.assertEquals(taskBroker2.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.JOB),
        resource);
    Assert.assertEquals(jobBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.JOB),
        resource);

    // Using different key returns a different object
    Assert.assertNotEquals(taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("otherKey"), GobblinScopes.JOB),
        resource);
    // Using different scope returns different object
    Assert.assertNotEquals(taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.TASK),
        resource);
    // Requesting unscoped resource returns different object
    Assert.assertNotEquals(taskBroker.getSharedResource(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey")),
        resource);
  }

  @Test
  public void testConfigurationInjection() throws Exception {

    String key = "myKey";

    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key1"), "value1",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, "key2"), "value2",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopes.CONTAINER.name(), "key2"), "value2scope",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, key, "key2"), "value2key",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, TestFactory.NAME, GobblinScopes.CONTAINER.name(), key, "key2"), "value2scopekey"
    ));

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopes.CONTAINER, "thisContainer").build();

    // create a shared resource
    TestFactory.SharedResource resource =
        containerBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.CONTAINER);

    Assert.assertEquals(resource.getConfig().getString("key1"), "value1");
    Assert.assertEquals(resource.getConfig().getString("key2"), "value2scopekey");
  }

  @Test
  public void testScoping() throws Exception {
    // Correct creation behavior
    Config config = ConfigFactory.empty();

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();

    Assert.assertEquals(jobBroker.getScope(GobblinScopes.INSTANCE).getType(), GobblinScopes.INSTANCE);
    Assert.assertEquals(jobBroker.getScope(GobblinScopes.INSTANCE).getScopeId(), GobblinScopes.INSTANCE.defaultId());
    Assert.assertEquals(jobBroker.getScope(GobblinScopes.JOB).getType(), GobblinScopes.JOB);
    Assert.assertEquals(jobBroker.getScope(GobblinScopes.JOB).getScopeId(), "job123");

    try {
      jobBroker.getScope(GobblinScopes.TASK);
      Assert.fail();
    } catch (NoSuchScopeException nsse) {
      // should throw no scope exception
    }
  }

  @Test
  public void testLifecycle() throws Exception {
    Config config = ConfigFactory.empty();

    SharedResourcesBrokerImpl<GobblinScopes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config);
    SharedResourcesBrokerImpl<GobblinScopes> jobBroker = topBroker.newSubscopedBuilder(GobblinScopes.JOB, "job123").build();
    SharedResourcesBrokerImpl<GobblinScopes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopes.CONTAINER, "thisContainer").build();
    SharedResourcesBrokerImpl<GobblinScopes> taskBroker = jobBroker.newSubscopedBuilder(GobblinScopes.TASK, "taskabc")
        .withAdditionalParentBroker(containerBroker).build();

    // create a shared resource
    TestFactory.SharedResource jobResource =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.JOB);
    TestFactory.SharedResource taskResource =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.TASK);

    Assert.assertFalse(jobResource.isClosed());
    Assert.assertFalse(taskResource.isClosed());

    taskBroker.close();

    // only resources at lower scopes than task should be closed
    Assert.assertFalse(jobResource.isClosed());
    Assert.assertTrue(taskResource.isClosed());

    // since taskResource has been closed, broker should return a new instance of the object
    TestFactory.SharedResource taskResource2 =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopes>(), new TestResourceKey("myKey"), GobblinScopes.TASK);
    Assert.assertNotEquals(taskResource, taskResource2);

    topBroker.close();

    Assert.assertTrue(jobResource.isClosed());
    Assert.assertTrue(taskResource.isClosed());
  }
}
