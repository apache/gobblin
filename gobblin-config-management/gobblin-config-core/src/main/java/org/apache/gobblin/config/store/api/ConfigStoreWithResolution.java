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

package gobblin.config.store.api;

import java.util.List;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;


/**
 * The ConfigStoreWithResolution interface is used to indicate the {@link ConfigStore} implementation
 * supports an efficient import configuration resolution for a given config key. The library will
 * delegate the import resolution to this implementation instead of performing it.
 *
 * The resolution is performed by using the
 * {@link Config#withFallback(com.typesafe.config.ConfigMergeable)} in the correct order. See the
 * package documentation for more information on the order of import resolution.
 *
 * @author mitu
 *
 */
@Alpha
public interface ConfigStoreWithResolution extends ConfigStore {

  /**
   * Obtains a {@link Config} object with all implicit and explicit imports resolved, i.e. specified
   * using the {@link Config#withFallback(com.typesafe.config.ConfigMergeable)} API.
   *
   * @param  configKey       the path of the configuration key to be resolved
   * @param  version         the configuration version for resolution
   * @return the {@link Config} object associated with the specified config key with akk direct
   *         and indirect imports resolved.
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   */
  public Config getResolvedConfig(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException;

  /**
   * Obtains the list of config keys which are directly and indirectly imported by the specified
   * config key. The import graph is traversed in depth-first manner. For a given config key,
   * explicit imports are listed before implicit imports from the ancestor keys.
   *
   * @param  configKey      the path of the config key whose imports are needed
   * @param  version        the configuration version to check
   * @return the paths of the directly and indirectly imported keys, including config keys imported
   *         by ancestors. The earlier config key in the list will have higher priority when resolving
   *         configuration conflict.
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   */
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException;
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey, String version, Optional<Config> runtimeConfig)
      throws VersionDoesNotExistException;

}
