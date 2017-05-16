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

import java.net.URI;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import gobblin.annotation.Alpha;


/**
 * The ConfigStore interface used to describe a configuration store. A configuration store is a
 * responsible for:
 * <ul>
 *  <li>Storing and fetching the values for configuration keys
 *  <li>Storing and fetching the tags for configuration keys
 *  <li>Maintaining different versions of the above mappings
 * </ul>
 * This API defines the minimum functionality that a store has to implement. There are also
 * a number of additional APIS (such as {@link ConfigStoreWithBatchFetches},
 * {@link ConfigStoreWithImportedBy}, {@link ConfigStoreWithImportedByRecursively},
 * {@link ConfigStoreWithResolution}, {@link ConfigStoreWithStableVersioning}) that denote that the
 * store supports additional operations efficiently and the config client library should delegate
 * those to the store rather than implementing those itself.
 *
 * @author mitu
 *
 */
@Alpha
public interface ConfigStore {

  /**
   * @return the current version for that configuration store.
   */
  public String getCurrentVersion();

  /**
   * Obtains the config store root URI. This represents the logical location of the store.
   *
   * @return the configuration store root URI .
   */
  public URI getStoreURI();

  /**
   * Obtains the direct children config keys for a given config key. For example, the child
   * paths for /data/tracking may be /data/tracking/ImpressionEvent and /data/tracking/ClickEvent .
   *
   * <p>Note that this method should not be used for "service discovery", i.e. it need not return
   * all possible child paths but only those defined in the store. For example, the configuration
   * for /data/tracking/ConversionEvent may be implicitly inherited from /data/tracking and
   * /data/tracking/ConversionEvent may not be returned by this method.
   *
   * @param  configKey      the config key path whose children are necessary.
   * @param  version        specify the configuration version in the configuration store.
   * @return the direct children config key paths
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   */
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException;

  /**
   * Obtains the list of all config keys with which are given config key is tagged/annotated.
   *
   * @param  configKey      the config key path whose tags are needed
   * @param  version        the configuration version in the configuration store.
   * @return the paths of the directly imported config keys for the specified config key and
   * version. Note that order is significant the earlier ConfigKeyPath in the List will have higher priority
   * when resolving configuration conflicts.
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   *
   */
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException;

  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, String version, Optional<Config> runtimeConfig)
      throws VersionDoesNotExistException;

  /**
   * Obtains the configuration properties directly associated with a given config keys. These <b>
   * will not</b> include any properties/values which can be obtained from the ancestors or imported
   * config keys.
   *
   * @param  configKey      the config key path whose properties are needed.
   * @param  version        the configuration version in the configuration store.
   * @return the directly specified configuration in {@link Config} format for input uri
   *  against input configuration version
   * @throws VersionDoesNotExistException if the requested config version does not exist (any longer)
   */
  public Config getOwnConfig(ConfigKeyPath configKey, String version)
      throws VersionDoesNotExistException;
}
