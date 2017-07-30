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
import java.util.Map;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;


/**
 * The ConfigStoreValueInspector interface used to inspect the {@link com.typesafe.config.Config} for a given
 * {@link gobblin.config.store.api.ConfigKeyPath} in {@link ConfigStore}
 *
 * @author mitu
 *
 */

public interface ConfigStoreValueInspector {

  /**
   * Obtains the configuration properties directly associated with a given config keys. These <b>
   * will not</b> include any properties/values which can be obtained from the ancestors or imported
   * config keys.
   *
   * @param  configKey      the config key path whose properties are needed.
   * @return the directly specified configuration in {@link Config} format for input configKey
   */
  public Config getOwnConfig(ConfigKeyPath configKey);

  /**
   * Obtains a {@link Config} object with all implicit and explicit imports resolved, i.e. specified
   * using the {@link Config#withFallback(com.typesafe.config.ConfigMergeable)} API.
   *
   * @param  configKey       the path of the configuration key to be resolved
   * @return the {@link Config} object associated with the specified config key with all direct
   *         and indirect imports resolved.
   */
  public Config getResolvedConfig(ConfigKeyPath configKey);

  public Config getResolvedConfig(ConfigKeyPath configKey, Optional<Config> runtimeConfig);

  /**
  *
  * @param  configKeys     the config keys whose {@link Config} objects are to be fetched
  * @return the Map from the config key to its own {@link com.typesafe.config.Config} object
  */
  public Map<ConfigKeyPath, Config> getOwnConfigs(Collection<ConfigKeyPath> configKeys);

  /**
  *
  * @param  configKeys     the config keys whose {@link Config} objects are to be fetched
  * @return the Map from the config key to its resolved {@link com.typesafe.config.Config} object
  */
  public Map<ConfigKeyPath, Config> getResolvedConfigs(Collection<ConfigKeyPath> configKeys);
}
