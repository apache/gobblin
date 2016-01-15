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

import java.util.List;

import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreWithResolution;

/**
 * ConfigStoreBackedValueInspector always query the underline {@link ConfigStore} to get the freshest 
 * {@link com.typesafe.config.Config}
 * @author mitu
 *
 */
public class ConfigStoreBackedValueInspector implements ConfigStoreValueInspector{

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
    return this.cs.getOwnConfig(configKey, version);
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
    if (this.cs instanceof ConfigStoreWithResolution) {
      return ((ConfigStoreWithResolution) this.cs).getResolvedConfig(configKey, this.version);
    }

    List<ConfigKeyPath> recursiveImports = this.topology.getImportsRecursively(configKey);
    Config initialConfig = this.getOwnConfig(configKey);
    
    // merge with other configs from imports
    for(ConfigKeyPath p: recursiveImports){
      initialConfig = initialConfig.withFallback(this.getConfigInSelfChain(p));
    }
    
    // merge with configs from parent
    initialConfig = initialConfig.withFallback(this.getConfigInSelfChain(configKey.getParent()));
    
    return initialConfig;
  }
  
  // return resolved config from self -> parent .. -> root
  private Config getConfigInSelfChain(ConfigKeyPath configKey){
    Config result = this.getOwnConfig(configKey);
    if(configKey.isRootPath())
      return result;
    
    return result.withFallback(getConfigInSelfChain(configKey.getParent()));
  }
}
