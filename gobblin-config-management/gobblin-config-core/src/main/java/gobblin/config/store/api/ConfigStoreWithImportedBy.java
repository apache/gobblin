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

import java.util.Collection;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;


/**
 * ConfigStoreWithImportedBy indicates that this {@link ConfigStore} implementation supports an
 * efficient mapping from the config key to the config keys that directly import it (aka the
 * imported-by relationship).
 * @author mitu
 *
 */
@Alpha
public interface ConfigStoreWithImportedBy extends ConfigStore {

  /**
   * Obtains the collection of config keys which import a given config key.
   *
   * @param  configKey   the config key path which is imported
   * @param  version     the configuration version to run the query against
   * @return The {@link Collection} of paths of the config keys which import the specified config key
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   */
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException;

  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey, String version, Optional<Config> runtimeConfig)
      throws VersionDoesNotExistException;
}
