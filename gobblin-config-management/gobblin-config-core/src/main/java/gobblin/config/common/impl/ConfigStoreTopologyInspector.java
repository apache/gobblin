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

/**
 * The ConfigStoreTopology interface used to describe the topology of a configuration store.
 *
 * Each node in the configuration store is represented as one {@link ConfigKeyPath}, so the topology of
 * any node includes:
 *
 * 1. The link to it's parent ( already defined in {@link ConfigKeyPath} )
 * 2. All the direct children of the node.
 * 3. The node's directly outgoing imports links. ( defined as getOwnImports() )
 * 4. Other nodes which directly imports the node. ( defined as getImportedBy() )
 * 5. The node's directly and indirectly imports links. ( defined as getImportsRecursively )
 * 6. Other nodes which directly and indirectly imports the node. ( defined as getImportedByRecursively() )
 *
 * @author mitu
 *
 */
public interface ConfigStoreTopologyInspector {

  /**
   * Obtains the direct children config keys for a given config key.
   *
   * @param  configKey      the config key path whose children are necessary.
   * @return the direct children config key paths
   */
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey);

  /**
   * @param configKey   the config key path which to get own imports.
   * @return the paths of the directly imported config keys for the specified config key
   * Note that order is significant the earlier ConfigKeyPath in the List will have higher priority
   * when resolving configuration conflicts.
   */
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey);

  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, Optional<Config> runtimeConfig);

  /**
   * Obtains the collection of config keys which import a given config key.
   *
   * @param  configKey   the config key path which is imported
   * @return The {@link Collection} of paths of the config keys which import the specified config key
   */
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey);

  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey, Optional<Config> runtimeConfig);

  /**
   * Obtains the list of config keys which are directly and indirectly imported by the specified
   * config key. The import graph is traversed in depth-first manner. For a given config key,
   * explicit imports are listed before implicit imports from the ancestor keys.
   *`
   * @param  configKey      the path of the config key whose imports are needed
   * @return the paths of the directly and indirectly imported keys, including config keys imported
   *         by ancestors. The earlier config key in the list will have higher priority when resolving
   *         configuration conflict.
   */
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey);

  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig);

  /**
   * Obtains all config keys which directly or indirectly import a given config key
   * @param  configKey      the path of the config key being imported
   * @return The {@link Collection} of paths of the config keys that directly or indirectly import
   *         the specified config key in the specified conf version.
   */
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey);

  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig);
}
