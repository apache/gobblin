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

package gobblin.config.common.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreWithBatchFetches;
import gobblin.config.store.api.ConfigStoreWithResolution;


/**
 * ConfigStoreBackedValueInspector always query the underline {@link ConfigStore} to get the freshest
 * {@link com.typesafe.config.Config}
 * @author mitu
 *
 */
public class ConfigStoreBackedValueInspector implements ConfigStoreValueInspector {

  private final ConfigStore cs;
  private final String version;
  private final ConfigStoreTopologyInspector topology;

  /**
   * @param cs       - internal {@link ConfigStore} to retrieve configuration
   * @param version  - version of the {@link ConfigStore}
   * @param topology - corresponding {@link ConfigStoreTopologyInspector} for the input {@link ConfigStore}
   */
  public ConfigStoreBackedValueInspector(ConfigStore cs, String version, ConfigStoreTopologyInspector topology) {
    this.cs = cs;
    this.version = version;
    this.topology = topology;
  }

  public ConfigStore getConfigStore() {
    return this.cs;
  }

  public String getVersion() {
    return this.version;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version
   * </p>
   */
  @Override
  public Config getOwnConfig(ConfigKeyPath configKey) {
    return this.cs.getOwnConfig(configKey, this.version);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version
   *   if the internal {@link ConfigStore} is {@link ConfigStoreWithBatchFetches}, otherwise, will call
   *   configuration store for each config key path and put the result into {@link Map}
   * </p>
   */
  @Override
  public Map<ConfigKeyPath, Config> getOwnConfigs(Collection<ConfigKeyPath> configKeys) {
    if (this.cs instanceof ConfigStoreWithBatchFetches) {
      ConfigStoreWithBatchFetches batchStore = (ConfigStoreWithBatchFetches) this.cs;
      return batchStore.getOwnConfigs(configKeys, this.version);
    }

    Map<ConfigKeyPath, Config> result = new HashMap<>();
    for (ConfigKeyPath configKey : configKeys) {
      result.put(configKey, this.cs.getOwnConfig(configKey, this.version));
    }

    return result;
  }

  @SuppressWarnings
  private Config getResolvedConfigRecursive(ConfigKeyPath configKey, Set<String> alreadyLoadedPaths) {
    return getResolvedConfigRecursive(configKey, alreadyLoadedPaths, Optional.<Config>absent());
  }

  private Config getResolvedConfigRecursive(ConfigKeyPath configKey, Set<String> alreadyLoadedPaths,
      Optional<Config> runtimeConfig) {

    if (this.cs instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution) this.cs).getResolvedConfig(configKey, this.version);
    }

    if (!alreadyLoadedPaths.add(configKey.getAbsolutePathString())) {
      return ConfigFactory.empty();
    }

    Config initialConfig = this.getOwnConfig(configKey);
    if (configKey.isRootPath()) {
      return initialConfig;
    }

    List<ConfigKeyPath> ownImports = this.topology.getOwnImports(configKey, runtimeConfig);
    // merge with other configs from imports
    if (ownImports != null) {
      for (ConfigKeyPath p : ownImports) {
        initialConfig =
            initialConfig.withFallback(this.getResolvedConfigRecursive(p, alreadyLoadedPaths, runtimeConfig));
      }
    }

    // merge with configs from parent for Non root
    initialConfig = initialConfig
        .withFallback(this.getResolvedConfigRecursive(configKey.getParent(), alreadyLoadedPaths, runtimeConfig));

    return initialConfig;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version if
   *   the internal {@link ConfigStore} is {@link ConfigStoreWithResolution}, otherwise based on {@link ConfigStoreTopologyInspector}
   *
   *   1. find out all the imports recursively
   *   2. resolved the config on the fly
   * </p>
   */
  public Config getResolvedConfig(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    return getResolvedConfigRecursive(configKey, Sets.<String>newHashSet(), runtimeConfig)
        .withFallback(ConfigFactory.defaultOverrides()).withFallback(ConfigFactory.systemEnvironment()).resolve();
  }

  @Override
  public Config getResolvedConfig(ConfigKeyPath configKey) {
    return getResolvedConfig(configKey, Optional.<Config>absent());
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version
   *   if the internal {@link ConfigStore} is {@link ConfigStoreWithBatchFetches}, otherwise, will call
   *   configuration store for each config key path and put the result into {@link Map}
   * </p>
   */
  @Override
  public Map<ConfigKeyPath, Config> getResolvedConfigs(Collection<ConfigKeyPath> configKeys) {
    if (this.cs instanceof ConfigStoreWithBatchFetches) {
      ConfigStoreWithBatchFetches batchStore = (ConfigStoreWithBatchFetches) this.cs;
      return batchStore.getResolvedConfigs(configKeys, this.version);
    }

    Map<ConfigKeyPath, Config> result = new HashMap<>();
    for (ConfigKeyPath configKey : configKeys) {
      result.put(configKey, this.getResolvedConfig(configKey));
    }

    return result;
  }

}
