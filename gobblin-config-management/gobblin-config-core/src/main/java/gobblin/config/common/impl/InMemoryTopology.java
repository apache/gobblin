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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import gobblin.config.store.api.ConfigKeyPath;


/**
 * InMemoryTopology will return stale data if the internal config store is Not {@ConfigStoreWithStableVersioning}
 * 
 * @author mitu
 *
 */
public class InMemoryTopology implements ConfigStoreTopologyInspector {

  private final ConfigStoreTopologyInspector fallback;

  // can not use Guava {@com.google.common.collect.MultiMap} as MultiMap does not store entry pair if the value is empty
  private final Map<ConfigKeyPath, Collection<ConfigKeyPath>> childrenMap = new HashMap<>();
  private final Map<ConfigKeyPath, List<ConfigKeyPath>> ownImportMap = new HashMap<>();
  private final Map<ConfigKeyPath, Collection<ConfigKeyPath>> ownImportedByMap = new HashMap<>();
  private final Map<ConfigKeyPath, List<ConfigKeyPath>> recursiveImportMap = new HashMap<>();
  private final Map<ConfigKeyPath, Collection<ConfigKeyPath>> recursiveImportedByMap = new HashMap<>();

  public InMemoryTopology(ConfigStoreTopologyInspector fallback) {
    this.fallback = fallback;
  }

  private void loadRawTopologyFromFallBack() {
    // breath first search the whole topology to build ownImports map and ownImportedByMap
    // calls to retrieve cache / set cache if not present
    Collection<ConfigKeyPath> currentLevel = this.getChildren(SingleLinkedListConfigKeyPath.ROOT);

    List<ConfigKeyPath> rootImports = this.getOwnImports(SingleLinkedListConfigKeyPath.ROOT);
    Preconditions.checkArgument(rootImports == null || rootImports.size() == 0,
        "Root can not import other nodes, otherwise circular dependency will happen");

    while (!currentLevel.isEmpty()) {
      Collection<ConfigKeyPath> nextLevel = new ArrayList<ConfigKeyPath>();
      for (ConfigKeyPath configKeyPath : currentLevel) {
        // calls to retrieve cache / set cache if not present
        List<ConfigKeyPath> ownImports = this.getOwnImports(configKeyPath);
        for (ConfigKeyPath p : ownImports) {
          this.addToCollectionMapForSingleValue(this.ownImportedByMap, p, configKeyPath);
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
        this.addToListMapForListValue(recursiveImportMap, configKey, recursiveImports);
      }

      if (recursiveImports != null) {
        for (ConfigKeyPath p : recursiveImports) {
          this.addToCollectionMapForSingleValue(this.recursiveImportedByMap, p, configKey);
        }
      }
    }
  }

  private List<ConfigKeyPath> buildImportsRecursiveFromCache(ConfigKeyPath configKey) {
    return buildImportsRecursiveFromCacheHelper(configKey, configKey, new ArrayList<ConfigKeyPath>());
  }

  private List<ConfigKeyPath> buildImportsRecursiveFromCacheHelper(ConfigKeyPath initialConfigKey,
      ConfigKeyPath currentConfigKey, List<ConfigKeyPath> previous) {

    for (ConfigKeyPath p : previous) {
      if (currentConfigKey != null && currentConfigKey.equals(p)) {
        previous.add(p);
        throw new CircularDependencyException(getCircularDependencyChain(initialConfigKey, previous, currentConfigKey));
      }
    }

    // root can not include anything, otherwise will have circular dependency
    if (currentConfigKey.isRootPath()) {
      return Collections.emptyList();
    }

    List<ConfigKeyPath> result = new ArrayList<>();

    // Own imports map should be filled in already
    List<ConfigKeyPath> rawList = this.getOwnImportsFromCache(currentConfigKey);

    // without this wrapper, will cause UnsupportedOperationException in adding parent node
    List<ConfigKeyPath> imported = new ArrayList<ConfigKeyPath>();
    imported.addAll(rawList);

    // implicit import parent with the lowest priority
    if (!currentConfigKey.isRootPath()) {
      imported.add(currentConfigKey.getParent());
    }

    for (ConfigKeyPath u : imported) {
      // do NOT add self parent in result as
      // 1. by default import parent
      // 2. if added, too many entries in the result
      if (!u.equals(currentConfigKey.getParent())) {
        result.add(u);
      }

      List<ConfigKeyPath> current = new ArrayList<ConfigKeyPath>();
      current.addAll(previous);
      current.add(currentConfigKey);

      List<ConfigKeyPath> subResult = null;
      if (this.recursiveImportMap.containsKey(u)) {
        subResult = this.recursiveImportMap.get(u);
      } else {
        subResult = buildImportsRecursiveFromCacheHelper(initialConfigKey, u, current);
        this.addToListMapForListValue(this.recursiveImportMap, u, subResult);
      }

      result.addAll(subResult);
    }

    return dedup(result);
  }

  private void addToCollectionMapForCollectionValue(Map<ConfigKeyPath, Collection<ConfigKeyPath>> theMap,
      ConfigKeyPath key, Collection<ConfigKeyPath> value) {
    if (theMap.containsKey(key)) {
      theMap.get(key).addAll(value);
    } else {
      theMap.put(key, value);
    }
  }

  private void addToCollectionMapForSingleValue(Map<ConfigKeyPath, Collection<ConfigKeyPath>> theMap,
      ConfigKeyPath key, ConfigKeyPath value) {
    if (theMap.containsKey(key)) {
      theMap.get(key).add(value);
    } else {
      List<ConfigKeyPath> list = new ArrayList<ConfigKeyPath>();
      list.add(value);
      theMap.put(key, list);
    }
  }

  private void addToListMapForListValue(Map<ConfigKeyPath, List<ConfigKeyPath>> theMap, ConfigKeyPath key,
      List<ConfigKeyPath> value) {
    if (theMap.containsKey(key)) {
      theMap.get(key).addAll(value);
    } else {
      theMap.put(key, value);
    }
  }

  private void addToListMapForSingleValue(Map<ConfigKeyPath, List<ConfigKeyPath>> theMap, ConfigKeyPath key,
      ConfigKeyPath value) {
    if (theMap.containsKey(key)) {
      theMap.get(key).add(value);
    } else {
      List<ConfigKeyPath> list = new ArrayList<ConfigKeyPath>();
      list.add(value);
      theMap.put(key, list);
    }
  }

  private static List<ConfigKeyPath> dedup(List<ConfigKeyPath> input) {
    List<ConfigKeyPath> result = new ArrayList<ConfigKeyPath>();

    Set<ConfigKeyPath> alreadySeen = new HashSet<ConfigKeyPath>();
    for (ConfigKeyPath u : input) {
      if (!alreadySeen.contains(u)) {
        result.add(u);
        alreadySeen.add(u);
      }
    }
    return result;
  }

  // return the circular dependency chain
  private static String getCircularDependencyChain(ConfigKeyPath initialConfigKey, List<ConfigKeyPath> chain,
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
    this.addToCollectionMapForCollectionValue(this.childrenMap, configKey, result);
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
    if (this.ownImportMap.containsKey(configKey)) {
      return this.ownImportMap.get(configKey);
    }

    List<ConfigKeyPath> result = this.fallback.getOwnImports(configKey);
    this.addToListMapForListValue(this.ownImportMap, configKey, result);
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
   *   If the fallback did not support this operation, will build the entire topology of the {@ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey) {
    if (this.ownImportedByMap.containsKey(configKey)) {
      return this.ownImportedByMap.get(configKey);
    }

    try {
      Collection<ConfigKeyPath> result = this.fallback.getImportedBy(configKey);
      this.addToCollectionMapForCollectionValue(this.ownImportedByMap, configKey, result);
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
   *   If the fallback did not support this operation, will build the entire topology of the {@ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey) {
    if (this.recursiveImportMap.containsKey(configKey)) {
      return this.recursiveImportMap.get(configKey);
    }

    try {
      List<ConfigKeyPath> result = this.fallback.getImportsRecursively(configKey);
      this.addToListMapForListValue(this.recursiveImportMap, configKey, result);
      return result;
    } catch (UnsupportedOperationException uoe) {
      loadRawTopologyFromFallBack();
      return this.recursiveImportMap.get(configKey);
    }
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object.
   *   
   *   If the fallback did not support this operation, will build the entire topology of the {@ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey) {
    if (this.recursiveImportedByMap.containsKey(configKey)) {
      return this.recursiveImportedByMap.get(configKey);
    }

    try {
      Collection<ConfigKeyPath> result = this.fallback.getImportedByRecursively(configKey);
      this.addToCollectionMapForCollectionValue(this.recursiveImportedByMap, configKey, result);
      return result;
    } catch (UnsupportedOperationException uoe) {
      loadRawTopologyFromFallBack();
      return this.recursiveImportedByMap.get(configKey);
    }
  }

}
