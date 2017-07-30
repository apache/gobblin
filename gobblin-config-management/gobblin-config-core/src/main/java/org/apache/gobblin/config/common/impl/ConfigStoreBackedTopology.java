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
import java.util.List;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreWithImportedBy;
import gobblin.config.store.api.ConfigStoreWithImportedByRecursively;
import gobblin.config.store.api.ConfigStoreWithResolution;

public class ConfigStoreBackedTopology implements ConfigStoreTopologyInspector {

  private final ConfigStore cs;
  private final String version;

  public ConfigStoreBackedTopology(ConfigStore cs, String version) {
    this.cs = cs;
    this.version = version;
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
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey) {
    return this.cs.getChildren(configKey, this.version);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey) {
    return getOwnImports(configKey, Optional.<Config>absent());
  }

  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (runtimeConfig.isPresent()) {
      return this.cs.getOwnImports(configKey, this.version, runtimeConfig);
    }
    return this.cs.getOwnImports(configKey, this.version);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version if
   *   the internal {@link ConfigStore} is {@link ConfigStoreWithImportedBy}, otherwise throws {@link UnsupportedOperationException}
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey) {
    return getImportedBy(configKey, Optional.<Config>absent());
  }

  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.cs instanceof ConfigStoreWithImportedBy) {
      return ((ConfigStoreWithImportedBy) this.cs).getImportedBy(configKey, this.version, runtimeConfig);
    }

    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version if
   *   the internal {@link ConfigStore} is {@link ConfigStoreWithResolution}, otherwise throws {@link UnsupportedOperationException}
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey) {
    return getImportsRecursively(configKey, Optional.<Config>absent());
  }

  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.cs instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution) this.cs).getImportsRecursively(configKey, this.version, runtimeConfig);
    }

    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@link ConfigStore}/version if
   *   the internal {@link ConfigStore} is {@link ConfigStoreWithImportedByRecursively}, otherwise throws {@link UnsupportedOperationException}
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey) {
    return getImportedByRecursively(configKey, Optional.<Config>absent());
  }

  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.cs instanceof ConfigStoreWithImportedByRecursively) {
      return ((ConfigStoreWithImportedByRecursively) this.cs).getImportedByRecursively(configKey, this.version, runtimeConfig);
    }

    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }
}
