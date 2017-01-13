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

package gobblin.broker;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.broker.iface.ScopeType;
import gobblin.broker.iface.ScopedConfigView;
import gobblin.broker.iface.SharedResourceKey;
import gobblin.util.ConfigUtils;

import javax.annotation.Nullable;
import lombok.Data;


/**
 * An implementation of {@link ScopedConfigView} that knows how to extract relevant subconfiguration from an input
 * {@link Config}.
 */
@Data
public class KeyedScopedConfigViewImpl<S extends ScopeType<S>, K extends SharedResourceKey>
    implements ScopedConfigView<S, K> {

  private static final Joiner JOINER = Joiner.on(".");

  @Nullable private final S scope;
  private final K key;
  private final String factoryName;
  private final Config fullConfig;

  public Config getFactorySpecificConfig() {
    return this.fullConfig;
  }

  public Config getScopedConfig() {
    if (this.scope == null) {
      return ConfigFactory.empty();
    }
    return ConfigUtils.getConfigOrEmpty(this.fullConfig, this.scope.name());
  }

  public Config getKeyedConfig() {
    return ConfigUtils.getConfigOrEmpty(this.fullConfig, this.key.toConfigurationKey());
  }

  public Config getKeyedScopedConfig() {
    if (this.scope == null) {
      return ConfigFactory.empty();
    }
    return ConfigUtils.getConfigOrEmpty(this.fullConfig,
        chainConfigKeys(this.scope.name(), this.key.toConfigurationKey()));
  }

  @Override
  public Config getConfig() {
    return getKeyedScopedConfig().withFallback(getKeyedConfig()).withFallback(getScopedConfig()).withFallback(getFactorySpecificConfig());
  }

  @Override
  public ScopedConfigView<S, K> getScopedView(S scopeType) {
    return new KeyedScopedConfigViewImpl<>(scopeType, this.key, this.factoryName, this.fullConfig);
  }

  private static String chainConfigKeys(String... keys) {
    return JOINER.join(keys);
  }
}
