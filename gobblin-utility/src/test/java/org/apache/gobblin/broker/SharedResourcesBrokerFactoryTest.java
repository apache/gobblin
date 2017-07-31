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

package org.apache.gobblin.broker;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.broker.iface.ConfigView;
import org.apache.gobblin.broker.iface.SharedResourcesBroker;

import lombok.RequiredArgsConstructor;


public class SharedResourcesBrokerFactoryTest {

  private static final SharedResourcesBroker<?> IMPLICIT = SharedResourcesBrokerFactory.getImplicitBroker();

  @Test
  public void testImplicitBroker() {

    Assert.assertEquals(SharedResourcesBrokerFactory.getImplicitBroker(), IMPLICIT);

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<?> future = executorService.submit(new ImplicitBrokerTest());
    try {
      future.get();
    } catch (ExecutionException | InterruptedException ee) {
      throw new RuntimeException(ee);
    }
    executorService.shutdownNow();

  }

  @Test
  public void testLoadingOfClasspath() {
    Config config =
        ConfigFactory.parseMap(ImmutableMap.of(SharedResourcesBrokerFactory.BROKER_CONF_FILE_KEY, "/broker/testBroker.conf"));
    SharedResourcesBrokerImpl<SimpleScopeType> broker =
        SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config, SimpleScopeType.GLOBAL.defaultScopeInstance());

    ConfigView configView = broker.getConfigView(null, null, "factory");
    Assert.assertTrue(configView.getConfig().hasPath("testKey"));
    Assert.assertEquals(configView.getConfig().getString("testKey"), "testValue");
  }

  public static class ImplicitBrokerTest implements Runnable {

    @Override
    public void run() {
      Assert.assertEquals(SharedResourcesBrokerFactory.getImplicitBroker(), IMPLICIT);

      SharedResourcesBroker<SimpleScopeType> broker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(
          ConfigFactory.empty(), SimpleScopeType.GLOBAL.defaultScopeInstance());

      Assert.assertNotEquals(SharedResourcesBrokerFactory.getImplicitBroker(), broker);
      SharedResourcesBrokerFactory.registerImplicitBroker(broker);
      Assert.assertEquals(SharedResourcesBrokerFactory.getImplicitBroker(), broker);

      ExecutorService executorService = Executors.newSingleThreadExecutor();
      Future<?> future = executorService.submit(new InnerImplicitBrokerTest(broker));
      try {
        future.get();
      } catch (ExecutionException | InterruptedException ee) {
        throw new RuntimeException(ee);
      }
      executorService.shutdownNow();

    }
  }

  @RequiredArgsConstructor
  public static class InnerImplicitBrokerTest implements  Runnable {
    private final SharedResourcesBroker<?> expectedBroker;

    @Override
    public void run() {
      Assert.assertEquals(this.expectedBroker, SharedResourcesBrokerFactory.getImplicitBroker());
    }
  }

}
