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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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

  private Config getResolvedConfigRecursive(ConfigKeyPath configKey) {

    if (this.cs instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution) this.cs).getResolvedConfig(configKey, this.version);
    }

    /**
     * currently use this function to check the circular dependency for the entire store, the result
     * is NOT used
     *
     *     root
     *    /
     *   l1 -> t1 (imports t1)
     *   /
     *   l2 -> t2 (imports t2)
     *
     * getImportsRecursively(l2) will return {t2,t1} as current implementation did NOT return the implicit
     * imports ( l1 ), otherwise, there will be a lot of result for both getImportsRecursively and
     * getImportedByRecursively
     *
     * if we use the result, the getResolvedConfig may equals
     * l2.ownConfig withFallback t2.ownConfig withFallback t1.ownConfig withFallback l1.ownConfig
     *
     *  but the correct result should be
     *  l2.ownConfig withFallback t2.ownConfig withFallback l1.ownConfig withFallback t1.ownConfig
     *
     *  The wrong ordering for those is because of we did NOT include the implicit imports l1
     */
    this.topology.getImportsRecursively(configKey);

    Config initialConfig = this.getOwnConfig(configKey);
    if (configKey.isRootPath()) {
      return initialConfig;
    }

    List<ConfigKeyPath> ownImports = this.topology.getOwnImports(configKey);
    // merge with other configs from imports
    if (ownImports != null) {
      for (ConfigKeyPath p : ownImports) {
        initialConfig = initialConfig.withFallback(this.getResolvedConfigRecursive(p));
      }
    }

    // merge with configs from parent for Non root
    initialConfig = initialConfig.withFallback(this.getResolvedConfigRecursive(configKey.getParent()));

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
  @Override
  public Config getResolvedConfig(ConfigKeyPath configKey) {
    return getResolvedConfigRecursive(configKey).withFallback(ConfigFactory.defaultOverrides())
        .withFallback(ConfigFactory.systemEnvironment()).resolve();
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
