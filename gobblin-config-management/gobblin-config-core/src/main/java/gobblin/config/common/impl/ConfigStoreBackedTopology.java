/*
 * Copyright (C) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.config.common.impl;

import java.util.Collection;
import java.util.List;

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
   *   This implementation simply delegate the functionality to the internal {@ConfigStore}/version
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
   *   This implementation simply delegate the functionality to the internal {@ConfigStore}/version
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey) {
    return this.cs.getOwnImports(configKey, this.version);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@ConfigStore}/version if
   *   the internal {@ConfigStore} is {@ConfigStoreWithImportedBy}, otherwise throws {@UnsupportedOperationException}
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey) {
    if (this.cs instanceof ConfigStoreWithImportedBy) {
      return ((ConfigStoreWithImportedBy) this.cs).getImportedBy(configKey, this.version);
    }

    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@ConfigStore}/version if
   *   the internal {@ConfigStore} is {@ConfigStoreWithResolution}, otherwise throws {@UnsupportedOperationException}
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey) {
    if (this.cs instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution) this.cs).getImportsRecursively(configKey, this.version);
    }

    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   This implementation simply delegate the functionality to the internal {@ConfigStore}/version if
   *   the internal {@ConfigStore} is {@ConfigStoreWithImportedByRecursively}, otherwise throws {@UnsupportedOperationException}
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey) {
    if (this.cs instanceof ConfigStoreWithImportedByRecursively) {
      return ((ConfigStoreWithImportedByRecursively) this.cs).getImportedByRecursively(configKey, this.version);
    }

    throw new UnsupportedOperationException("Internal ConfigStore does not support this operation");
  }
}
