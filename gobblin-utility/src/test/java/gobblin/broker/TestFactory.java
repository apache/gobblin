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

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.iface.ConfigView;
import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceFactory;
import gobblin.broker.iface.SharedResourcesBroker;

import lombok.Data;


public class TestFactory<S extends ScopeType<S>> implements SharedResourceFactory<TestFactory.SharedResource, TestResourceKey, S> {

  private static final Joiner JOINER = Joiner.on(".");
  private static final String AUTOSCOPE_AT = "autoscope.at";

  public static final String NAME = TestFactory.class.getSimpleName();

  public static Config setAutoScopeLevel(Config config, GobblinScopes level) {
    return ConfigFactory.parseMap(ImmutableMap.of(
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, NAME, AUTOSCOPE_AT), level.name()))
        .withFallback(config);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SharedResource createResource(SharedResourcesBroker broker, ScopedConfigView<?, TestResourceKey> config) {
    return new SharedResource(config.getKey().getKey(), config.getConfig());
  }

  @Override
  public S getAutoScope(SharedResourcesBroker<S> broker, ConfigView<S, TestResourceKey> config) {
    if (config.getConfig().hasPath(AUTOSCOPE_AT)) {
      return (S) GobblinScopes.valueOf(config.getConfig().getString(AUTOSCOPE_AT));
    } else {
      return broker.selfScope().getType();
    }
  }

  @Data
  public static class SharedResource implements Closeable {
    private final String key;
    private final Config config;
    private final long id = new Random().nextLong();
    private boolean closed = false;

    @Override
    public void close()
        throws IOException {
      if (this.closed) {
        throw new RuntimeException("Already closed.");
      }
      this.closed = true;
    }
  }

}
