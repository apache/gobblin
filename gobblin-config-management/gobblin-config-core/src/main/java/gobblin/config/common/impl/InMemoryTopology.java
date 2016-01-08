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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;

import gobblin.config.store.api.ConfigKeyPath;

/**
 * InMemoryTopology will return stale data if the internal config store is Not {@ConfigStoreWithStableVersioning}
 * 
 * @author mitu
 *
 */
public class InMemoryTopology implements ConfigStoreTopologyInspector {

  private final ConfigStoreTopologyInspector fallback;

  private final ListMultimap<ConfigKeyPath, ConfigKeyPath> childrenMap = ArrayListMultimap.create();
  private final ListMultimap<ConfigKeyPath, ConfigKeyPath> ownImportMap = ArrayListMultimap.create();
  private final HashMultimap<ConfigKeyPath, ConfigKeyPath> ownImportedByMap = HashMultimap.create();
  private final ListMultimap<ConfigKeyPath, ConfigKeyPath> recursiveImportMap = ArrayListMultimap.create();
  private final HashMultimap<ConfigKeyPath, ConfigKeyPath> recursiveImportedByMap = HashMultimap.create();

  public InMemoryTopology(ConfigStoreTopologyInspector fallback) {
    this.fallback = fallback;
  }

  private void loadRawTopologyFromFallBack() {
    // breath first search the whole topology to build ownImports map and ownImportedByMap
    // calls to retrieve cache / set cache if not present
    Collection<ConfigKeyPath> currentLevel = this.getChildren(SingleLinkedListConfigKeyPath.ROOT);

    while(!currentLevel.isEmpty()){
      Collection<ConfigKeyPath> nextLevel = new ArrayList<ConfigKeyPath>();
      for(ConfigKeyPath configKeyPath: currentLevel){
        // calls to retrieve cache / set cache if not present
        List<ConfigKeyPath> ownImports = this.getOwnImports(configKeyPath);
        for(ConfigKeyPath p: ownImports){
          this.ownImportedByMap.put(p, configKeyPath);
        }

        // calls to retrieve cache / set cache if not present
        Collection<ConfigKeyPath> tmp = this.getChildren(configKeyPath);
        nextLevel.addAll(tmp);
      }
      currentLevel = nextLevel;
    }

    System.out.println("size of ownImportMpa is " + this.ownImportMap.size() );
    System.out.println("size of chldrenMap is " + this.childrenMap.size() );
    // traversal the ownImports map to get the recursive imports map and set recursive imported by map
    for(ConfigKeyPath configKey: this.ownImportMap.keySet()){
      List<ConfigKeyPath> recursiveImports = null;
      // result may in the cache already 
      if(this.recursiveImportMap.containsKey(configKey)){
        recursiveImports = this.recursiveImportMap.get(configKey);
      }
      else {
        recursiveImports = this.buildImportsRecursiveFromCache(configKey);
        this.recursiveImportMap.putAll(configKey, recursiveImports); 
      }

      if(recursiveImports!=null){
        for(ConfigKeyPath p: recursiveImports){
          this.recursiveImportedByMap.put(p, configKey);
        }
      }
    }
  }

  private List<ConfigKeyPath> buildImportsRecursiveFromCache(ConfigKeyPath configKey){
    return buildImportsRecursiveFromCacheHelper(configKey, configKey, new ArrayList<ConfigKeyPath>());
  }
  
  private List<ConfigKeyPath> buildImportsRecursiveFromCacheHelper(ConfigKeyPath initialConfigKey, 
      ConfigKeyPath currentConfigKey, List<ConfigKeyPath> previous) {
    
    System.out.println("current is " + currentConfigKey);
    for (ConfigKeyPath p : previous) {
      System.out.println("previous is " + p);
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
    if(!currentConfigKey.isRootPath()){
      imported.add(currentConfigKey.getParent());
    }
    
    for (ConfigKeyPath u : imported) {
      // do NOT add self parent in result as
      // 1. by default import parent
      // 2. if added, too many entries in the result
      if(!u.equals(currentConfigKey.getParent())){
        result.add(u);
      }
      
      List<ConfigKeyPath> current = new ArrayList<ConfigKeyPath>();
      current.addAll(previous);
      current.add(currentConfigKey);
      
      List<ConfigKeyPath> subResult = null;
      if(this.recursiveImportMap.containsKey(u)){
        subResult = this.recursiveImportMap.get(u);
      }
      else {
        subResult = buildImportsRecursiveFromCacheHelper(initialConfigKey, u, current);
        this.recursiveImportMap.putAll(u, subResult); 
      }
      
      result.addAll(subResult);
    }

    return dedup(result);
  }
  
  private static List<ConfigKeyPath> dedup(List<ConfigKeyPath> input) {
    List<ConfigKeyPath> result = new ArrayList<ConfigKeyPath>();

    Set<ConfigKeyPath> alreadySeen = new HashSet<ConfigKeyPath>();
    for(ConfigKeyPath u: input){
      if(!alreadySeen.contains(u)){
        result.add(u);
        alreadySeen.add(u);
      }
    }
    return result;
  }

  // return the circular dependency chain
  private static String getCircularDependencyChain(ConfigKeyPath initialConfigKey, List<ConfigKeyPath> chain, ConfigKeyPath circular) {
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
    System.out.println("BBB " + configKey);
    if (this.childrenMap.containsKey(configKey)) {
      System.out.println("BBB in cache");
      return this.childrenMap.get(configKey);
    }

    Collection<ConfigKeyPath> result = this.fallback.getChildren(configKey);
    this.childrenMap.putAll(configKey, result);
    
    System.out.println("BBB size is " + result.size());
    System.out.println("BBB in cahche? " + this.childrenMap.containsKey(configKey));
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
    System.out.println("AAA " + configKey);
    if (this.ownImportMap.containsKey(configKey)) {
      System.out.println("AAA in cache");
      return this.ownImportMap.get(configKey);
    }

    List<ConfigKeyPath> result = this.fallback.getOwnImports(configKey);
    this.ownImportMap.putAll(configKey, result);
    
    System.out.println("AAA size is " + result.size());
    System.out.println("AAA in cahche? " + this.ownImportMap.containsKey(configKey));
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
      this.ownImportedByMap.putAll(configKey, result);
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
      this.recursiveImportMap.putAll(configKey, result);
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
      this.recursiveImportedByMap.putAll(configKey, result);
      return result;
    } catch (UnsupportedOperationException uoe) {
      loadRawTopologyFromFallBack();
      return this.recursiveImportedByMap.get(configKey);
    }
  }

}
