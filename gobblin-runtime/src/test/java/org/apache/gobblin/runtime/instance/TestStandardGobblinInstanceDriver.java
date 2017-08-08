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
package org.apache.gobblin.runtime.instance;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.runtime.api.GobblinInstanceDriver;
import org.apache.gobblin.runtime.api.GobblinInstanceEnvironment;
import org.apache.gobblin.runtime.api.GobblinInstancePlugin;
import org.apache.gobblin.runtime.api.GobblinInstancePluginFactory;
import org.apache.gobblin.runtime.plugins.email.EmailNotificationPlugin;
import org.apache.gobblin.runtime.std.DefaultConfigurableImpl;

import avro.shaded.com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Unit tests for {@link StandardGobblinInstanceDriver}
 */
public class TestStandardGobblinInstanceDriver {

  @Test
  public void testBuilder() {
    Config instanceCfg = ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
        .put(StandardGobblinInstanceDriver.PLUGINS_FULL_KEY, "fake1")
        .put(EmailNotificationPlugin.EMAIL_NOTIFICATIONS_DISABLED_KEY, Boolean.valueOf(true))
        .build());
    GobblinInstanceEnvironment mockEnv = Mockito.mock(GobblinInstanceEnvironment.class);
    Mockito.when(mockEnv.getSysConfig())
           .thenReturn(DefaultConfigurableImpl.createFromConfig(instanceCfg));
    StandardGobblinInstanceDriver.Builder builder =
        StandardGobblinInstanceDriver.builder()
        .withInstanceEnvironment(mockEnv)
        .addPlugin(new FakePluginFactory2());

    List<GobblinInstancePluginFactory> plugins = builder.getPlugins();
    Assert.assertEquals(plugins.size(), 2);

    Set<Class<?>> pluginClasses = new HashSet<>();
    pluginClasses.add(plugins.get(0).getClass());
    pluginClasses.add(plugins.get(1).getClass());

    Assert.assertTrue(pluginClasses.contains(FakePluginFactory1.class));
    Assert.assertTrue(pluginClasses.contains(FakePluginFactory2.class));
  }

  @AllArgsConstructor
  static class FakePlugin extends AbstractIdleService implements GobblinInstancePlugin {
    @Getter final GobblinInstanceDriver instance;

    @Override protected void startUp() throws Exception {
      // Do nothing
    }
    @Override protected void shutDown() throws Exception {
      // Do nothing
    }
  }

  @Alias("fake1")
  static class FakePluginFactory1 implements GobblinInstancePluginFactory {
    @Override public GobblinInstancePlugin createPlugin(GobblinInstanceDriver instance) {
      return new FakePlugin(instance);
    }
  }

  @Alias("fake2")
  static class FakePluginFactory2 implements GobblinInstancePluginFactory {
    @Override public GobblinInstancePlugin createPlugin(GobblinInstanceDriver instance) {
      return new FakePlugin(instance);
    }
  }

}
