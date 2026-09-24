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
package org.apache.gobblin.service.modules.core;

import java.util.List;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.service.ServiceConfigKeys;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.ForceKillHandler;


public class GobblinServiceForceKillConfigurationTest {
  @Test
  public void testNoConfiguredBackendLeavesOptionalAbsent() {
    Injector injector = injector(ConfigFactory.empty());

    Assert.assertFalse(injector.getInstance(Key.get(new TypeLiteral<Optional<ForceKillHandler>>() { })).isPresent());
    Assert.assertFalse(injector.getInstance(Key.get(
        new TypeLiteral<Optional<Provider<ForceKillHandler>>>() { })).isPresent());
  }

  @Test
  public void testConfiguredBackendIsInjectedAsOneSingletonForManagerAndFactory() {
    Config config = ConfigFactory.empty().withValue(ServiceConfigKeys.GOBBLIN_SERVICE_FORCE_KILL_HANDLER_CLASS_KEY,
        ConfigValueFactory.fromAnyRef(TestHandler.class.getName()));
    Injector injector = injector(config);

    ForceKillHandler managerHandler =
        injector.getInstance(Key.get(new TypeLiteral<Optional<ForceKillHandler>>() { })).get();
    Provider<ForceKillHandler> factoryProvider =
        injector.getInstance(Key.get(new TypeLiteral<Optional<Provider<ForceKillHandler>>>() { })).get();

    Assert.assertSame(managerHandler, factoryProvider.get());
    Assert.assertSame(managerHandler, factoryProvider.get());
    Assert.assertSame(((TestHandler) managerHandler).config, config);
  }

  private static Injector injector(Config config) {
    return Guice.createInjector(binder -> {
      binder.requireExplicitBindings();
      binder.bind(Config.class).toInstance(config);
      GobblinServiceGuiceModule.configureForceKillHandler(binder, config);
    });
  }

  public static class TestHandler implements ForceKillHandler {
    final Config config;

    @Inject
    public TestHandler(Config config) {
      this.config = config;
    }

    @Override
    public Result forceKill(DagActionStore.DagAction action, List<State> states, ActiveDagCheck guard) {
      return Result.ACKNOWLEDGED;
    }
  }
}
