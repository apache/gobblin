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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;


/**
 * InMemoryTopology will return stale data if the internal config store is Not {@link ConfigStoreWithStableVersioning}
 *
 * @author mitu
 *
 */
public class InMemoryTopology implements ConfigStoreTopologyInspector {

  private final ConfigStoreTopologyInspector fallback;

  // can not use Guava {@link com.google.common.collect.MultiMap} as MultiMap does not store entry pair if the value is empty
  private final Map<ConfigKeyPath, Collection<ConfigKeyPath>> childrenMap = new HashMap<>();
  private final Map<ConfigKeyPath, List<ConfigKeyPath>> ownImportMap = new HashMap<>();
  private final Map<ConfigKeyPath, Collection<ConfigKeyPath>> ownImportedByMap = new HashMap<>();
  private final Map<ConfigKeyPath, List<ConfigKeyPath>> recursiveImportMap = new HashMap<>();
  private final Map<ConfigKeyPath, Collection<ConfigKeyPath>> recursiveImportedByMap = new HashMap<>();
  private boolean initialedTopologyFromFallBack = false;

  public InMemoryTopology(ConfigStoreTopologyInspector fallback) {
    this.fallback = fallback;
  }

  private void loadRawTopologyFromFallBack() {
    loadRawTopologyFromFallBack(Optional.<Config>absent());
  }

  private void loadRawTopologyFromFallBack(Optional<Config> runtimeConfig) {
    // only initialize the topology once
    if (this.initialedTopologyFromFallBack) {
      return;
    }

    // breath first search the whole topology to build ownImports map and ownImportedByMap
    // calls to retrieve cache / set cache if not present
    Collection<ConfigKeyPath> currentLevel = this.getChildren(SingleLinkedListConfigKeyPath.ROOT);

    List<ConfigKeyPath> rootImports = this.getOwnImports(SingleLinkedListConfigKeyPath.ROOT, runtimeConfig);
    Preconditions.checkArgument(rootImports == null || rootImports.size() == 0,
        "Root can not import other nodes, otherwise circular dependency will happen");

    while (!currentLevel.isEmpty()) {
      Collection<ConfigKeyPath> nextLevel = new ArrayList<>();
      for (ConfigKeyPath configKeyPath : currentLevel) {
        // calls to retrieve cache / set cache if not present
        List<ConfigKeyPath> ownImports = this.getOwnImports(configKeyPath, runtimeConfig);
        for (ConfigKeyPath p : ownImports) {
          addToCollectionMapForSingleValue(this.ownImportedByMap, p, configKeyPath);
        }

        // calls to retrieve cache / set cache if not present
        Collection<ConfigKeyPath> tmp = this.getChildren(configKeyPath);
        nextLevel.addAll(tmp);
      }
      currentLevel = nextLevel;
    }

    // traversal the ownImports map to get the recursive imports map and set recursive imported by map
    for (ConfigKeyPath configKey : this.ownImportMap.keySet()) {
      List<ConfigKeyPath> recursiveImports = null;
      // result may in the cache already
      if (this.recursiveImportMap.containsKey(configKey)) {
        recursiveImports = this.recursiveImportMap.get(configKey);
      } else {
        recursiveImports = this.buildImportsRecursiveFromCache(configKey);
        addToListMapForListValue(this.recursiveImportMap, configKey, recursiveImports);
      }

      if (recursiveImports != null) {
        for (ConfigKeyPath p : recursiveImports) {
          addToCollectionMapForSingleValue(this.recursiveImportedByMap, p, configKey);
        }
      }
    }

    this.initialedTopologyFromFallBack = true;
  }

  private List<ConfigKeyPath> buildImportsRecursiveFromCache(ConfigKeyPath configKey) {
    return buildImportsRecursiveFromCacheHelper(configKey, configKey, new LinkedHashSet<ConfigKeyPath>());
  }

  private List<ConfigKeyPath> buildImportsRecursiveFromCacheHelper(ConfigKeyPath initialConfigKey,
      ConfigKeyPath currentConfigKey, Set<ConfigKeyPath> previousVisited) {

    for (ConfigKeyPath p : previousVisited) {
      if (currentConfigKey != null && currentConfigKey.equals(p)) {
        previousVisited.add(p);
        throw new CircularDependencyException(
            getCircularDependencyChain(initialConfigKey, previousVisited, currentConfigKey));
      }
    }

    // root can not include anything, otherwise will have circular dependency
    if (currentConfigKey.isRootPath()) {
      return Collections.emptyList();
    }

    List<ConfigKeyPath> result = new ArrayList<>();

    Set<ConfigKeyPath> currentVisited = new LinkedHashSet<>(previousVisited);
    currentVisited.add(currentConfigKey);

    // Own imports map should be filled in already
    for (ConfigKeyPath u : this.getOwnImportsFromCache(currentConfigKey)) {
      addRecursiveImportsToResult(u, initialConfigKey, currentConfigKey, result, currentVisited);
    }

    addRecursiveImportsToResult(currentConfigKey.getParent(), initialConfigKey, currentConfigKey, result,
        currentVisited);
    return dedup(result);
  }

  private void addRecursiveImportsToResult(ConfigKeyPath u, ConfigKeyPath initialConfigKey,
      ConfigKeyPath currentConfigKey, List<ConfigKeyPath> result, Set<ConfigKeyPath> current) {

    // do NOT add self parent in result as
    // 1. by default import parent
    // 2. if added, too many entries in the result
    if (!u.equals(currentConfigKey.getParent())) {
      result.add(u);
    }

    List<ConfigKeyPath> subResult = null;
    if (this.recursiveImportMap.containsKey(u)) {
      subResult = this.recursiveImportMap.get(u);
    } else {
      subResult = buildImportsRecursiveFromCacheHelper(initialConfigKey, u, current);
      addToListMapForListValue(this.recursiveImportMap, u, subResult);
    }

    result.addAll(subResult);
  }

  private static void addToCollectionMapForCollectionValue(Map<ConfigKeyPath, Collection<ConfigKeyPath>> theMap,
      ConfigKeyPath key, Collection<ConfigKeyPath> value) {
    if (theMap.containsKey(key)) {
      theMap.get(key).addAll(value);
    } else {
      theMap.put(key, value);
    }
  }

  private static void addToCollectionMapForSingleValue(Map<ConfigKeyPath, Collection<ConfigKeyPath>> theMap,
      ConfigKeyPath key, ConfigKeyPath value) {
    if (theMap.containsKey(key)) {
      theMap.get(key).add(value);
    } else {
      List<ConfigKeyPath> list = new ArrayList<>();
      list.add(value);
      theMap.put(key, list);
    }
  }

  private static void addToListMapForListValue(Map<ConfigKeyPath, List<ConfigKeyPath>> theMap, ConfigKeyPath key,
      List<ConfigKeyPath> value) {
    if (theMap.containsKey(key)) {
      theMap.get(key).addAll(value);
    } else {
      theMap.put(key, value);
    }
  }

  private static List<ConfigKeyPath> dedup(List<ConfigKeyPath> input) {
    List<ConfigKeyPath> result = new ArrayList<>();

    Set<ConfigKeyPath> alreadySeen = new HashSet<>();
    for (ConfigKeyPath u : input) {
      if (!alreadySeen.contains(u)) {
        result.add(u);
        alreadySeen.add(u);
      }
    }
    return result;
  }

  // return the circular dependency chain
  private static String getCircularDependencyChain(ConfigKeyPath initialConfigKey, Set<ConfigKeyPath> chain,
      ConfigKeyPath circular) {
    StringBuilder sb = new StringBuilder();
    sb.append("Initial configKey : " + initialConfigKey + ", loop is ");
    for (ConfigKeyPath u : chain) {
      sb.append(" -> " + u);
    }

    sb.append(" the configKey causing circular dependency: " + circular);
    return sb.toString();
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey) {
    if (this.childrenMap.containsKey(configKey)) {
      return this.childrenMap.get(configKey);
    }

    Collection<ConfigKeyPath> result = this.fallback.getChildren(configKey);
    addToCollectionMapForCollectionValue(this.childrenMap, configKey, result);
    return result;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey) {
    return getOwnImports(configKey, Optional.<Config>absent());
  }

  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.ownImportMap.containsKey(configKey)) {
      return this.ownImportMap.get(configKey);
    }

    List<ConfigKeyPath> result = this.fallback.getOwnImports(configKey, runtimeConfig);
    addToListMapForListValue(this.ownImportMap, configKey, result);
    return result;
  }

  private List<ConfigKeyPath> getOwnImportsFromCache(ConfigKeyPath configKey) {
    if (this.ownImportMap.containsKey(configKey)) {
      return this.ownImportMap.get(configKey);
    }

    return Collections.emptyList();
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object.
   *
   *   If the fallback did not support this operation, will build the entire topology of the {@link ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey) {
    return getImportedBy(configKey, Optional.<Config>absent());
  }

  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.ownImportedByMap.containsKey(configKey)) {
      return this.ownImportedByMap.get(configKey);
    }

    try {
      Collection<ConfigKeyPath> result = this.fallback.getImportedBy(configKey, runtimeConfig);
      addToCollectionMapForCollectionValue(this.ownImportedByMap, configKey, result);
      return result;
    } catch (UnsupportedOperationException uoe) {
      loadRawTopologyFromFallBack();
      return this.ownImportedByMap.get(configKey);
    }
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object.
   *
   *   If the fallback did not support this operation, will build the entire topology of the {@link ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey) {
    return getImportsRecursively(configKey, Optional.<Config>absent());
  }

  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.recursiveImportMap.containsKey(configKey)) {
      return this.recursiveImportMap.get(configKey);
    }

    try {
      List<ConfigKeyPath> result = this.fallback.getImportsRecursively(configKey, runtimeConfig);
      addToListMapForListValue(this.recursiveImportMap, configKey, result);
      return result;
    } catch (UnsupportedOperationException uoe) {
      loadRawTopologyFromFallBack();
      return this.getImportsRecursivelyIncludePhantomFromCache(configKey);
    }
  }

  // for phantom node, need to return the result of the parent
  private List<ConfigKeyPath> getImportsRecursivelyIncludePhantomFromCache(ConfigKeyPath configKey) {
    if (this.recursiveImportMap.containsKey(configKey)) {
      return this.recursiveImportMap.get(configKey);
    }

    if (configKey.isRootPath()) {
      return Collections.emptyList();
    }

    return getImportsRecursivelyIncludePhantomFromCache(configKey.getParent());
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object.
   *
   *   If the fallback did not support this operation, will build the entire topology of the {@link ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey) {
    return getImportedByRecursively(configKey, Optional.<Config>absent());
  }

  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.recursiveImportedByMap.containsKey(configKey)) {
      return this.recursiveImportedByMap.get(configKey);
    }

    try {
      Collection<ConfigKeyPath> result = this.fallback.getImportedByRecursively(configKey, runtimeConfig);
      addToCollectionMapForCollectionValue(this.recursiveImportedByMap, configKey, result);
      return result;
    } catch (UnsupportedOperationException uoe) {
      loadRawTopologyFromFallBack(runtimeConfig);
      return this.recursiveImportedByMap.get(configKey);
    }
  }

}
