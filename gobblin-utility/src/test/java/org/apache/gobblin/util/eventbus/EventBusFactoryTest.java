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

package org.apache.gobblin.util.eventbus;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.SharedResourcesBrokerFactory;
import org.apache.gobblin.broker.SharedResourcesBrokerImpl;
import org.apache.gobblin.broker.SimpleScope;
import org.apache.gobblin.broker.SimpleScopeType;
import org.apache.gobblin.broker.iface.NoSuchScopeException;
import org.apache.gobblin.broker.iface.NotConfiguredException;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;


public class EventBusFactoryTest {

  @Test
  public void testGet()
      throws NotConfiguredException, IOException, NoSuchScopeException {
    SharedResourcesBrokerImpl<SimpleScopeType> broker = SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(
        ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

    EventBus eventBus1 = EventBusFactory.get(getClass().getSimpleName(), broker);
    EventBus eventBus2 = EventBusFactory.get(getClass().getSimpleName(), broker);

    //Should return the same eventbus instance
    Assert.assertEquals(eventBus1, eventBus2);

    SharedResourcesBroker<SimpleScopeType> subBroker =
        broker.newSubscopedBuilder(new SimpleScope<>(SimpleScopeType.LOCAL, "local")).build();
    EventBus eventBus3 = EventBusFactory.get(getClass().getSimpleName(), subBroker);
    //Should return the same eventbus instance
    Assert.assertEquals(eventBus1, eventBus3);

    //Create a new eventbus with local scope
    EventBus eventBus4 = subBroker.getSharedResourceAtScope(new EventBusFactory<>(), new EventBusKey(getClass().getSimpleName()), SimpleScopeType.LOCAL);
    Assert.assertNotEquals(eventBus3, eventBus4);

    //Create an eventbus with different source class name
    EventBus eventBus5 = EventBusFactory.get("", broker);
    Assert.assertNotEquals(eventBus1, eventBus5);
  }
}