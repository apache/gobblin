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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * InMemoryTopology will return stale data if the internal config store is Not {@link gobblin.config.store.api.ConfigStoreWithStableVersioning}
 *
 * @author ibuenros
 *
 */
public class InMemoryTopology implements ConfigStoreTopologyInspector {

  private final ConfigStoreTopologyInspector fallback;

  // can not use Guava {@link com.google.common.collect.MultiMap} as MultiMap does not store entry pair if the value is empty
  private final Cache<ConfigKeyPath, Collection<ConfigKeyPath>> childrenMap = CacheBuilder.newBuilder().build();
  private final Cache<ConfigKeyPath, List<ConfigKeyPath>> ownImportMap = CacheBuilder.newBuilder().build();
  private final Cache<ConfigKeyPath, LinkedList<ConfigKeyPath>> recursiveImportMap = CacheBuilder.newBuilder().build();
  private final Cache<ConfigKeyPath, LinkedList<ConfigKeyPath>> recursiveImportedByMap = CacheBuilder.newBuilder().build();
  private final Cache<ConfigKeyPath, Collection<ConfigKeyPath>> ownImportedByMap = CacheBuilder.newBuilder().build();

  @SuppressFBWarnings(value = "IS2_INCONSISTENT_SYNC", justification = "Access is in fact thread safe.")
  private ImmutableMultimap<ConfigKeyPath, ConfigKeyPath> fullImportedByMap = null;

  public InMemoryTopology(ConfigStoreTopologyInspector fallback) {
    this.fallback = fallback;
  }

  private synchronized void computeImportedByMap(Optional<Config> runtimeConfig) {
    if (this.fullImportedByMap != null) {
      return;
    }

    ImmutableMultimap.Builder<ConfigKeyPath, ConfigKeyPath> importedByBuilder = ImmutableMultimap.builder();

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
        this.ownImportMap.put(configKeyPath, ownImports);
        for (ConfigKeyPath importedPath : ownImports) {
          importedByBuilder.put(importedPath, configKeyPath);
        }

        // calls to retrieve cache / set cache if not present
        Collection<ConfigKeyPath> tmp = this.getChildren(configKeyPath);
        nextLevel.addAll(tmp);
      }
      currentLevel = nextLevel;
    }

    this.fullImportedByMap = importedByBuilder.build();
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
    try {
      return this.childrenMap.get(configKey, () -> this.fallback.getChildren(configKey));
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
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
    try {
      return this.ownImportMap.get(configKey, () -> this.fallback.getOwnImports(configKey, runtimeConfig));
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object.
   *
   *   If the fallback did not support this operation, will build the entire topology of the {@link gobblin.config.store.api.ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey) {
    return getImportedBy(configKey, Optional.<Config>absent());
  }

  public Collection<ConfigKeyPath> getImportedBy(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    if (this.fullImportedByMap != null) {
      return this.fullImportedByMap.get(configKey);
    }

    try {
      return this.ownImportedByMap.get(configKey, () -> this.fallback.getImportedBy(configKey, runtimeConfig));
    } catch (UncheckedExecutionException exc) {
      if (exc.getCause() instanceof UnsupportedOperationException) {
        computeImportedByMap(runtimeConfig);
        return getImportedBy(configKey, runtimeConfig);
      } else {
        throw new RuntimeException(exc);
      }
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object.
   *
   *   If the fallback did not support this operation, will build the entire topology of the {@link gobblin.config.store.api.ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey) {
    return getImportsRecursively(configKey, Optional.<Config>absent());
  }

  public List<ConfigKeyPath> getImportsRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    return new ImportTraverser<>(key -> {

      if (key.isRootPath()) {
        return new LinkedList<>();
      }

      List<ConfigKeyPath> imports = Lists.newArrayList();
      imports.addAll(Lists.reverse(getOwnImports(key, runtimeConfig)));
      imports.add(key.getParent());

      return imports;
    }, this.recursiveImportMap).traverseGraphRecursively(configKey);
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If the result is already in cache, return the result.
   *   Otherwise, delegate the functionality to the fallback object.
   *
   *   If the fallback did not support this operation, will build the entire topology of the {@link gobblin.config.store.api.ConfigStore}
   *   using default breath first search.
   * </p>
   */
  @Override
  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey) {
    return getImportedByRecursively(configKey, Optional.<Config>absent());
  }

  public Collection<ConfigKeyPath> getImportedByRecursively(ConfigKeyPath configKey, Optional<Config> runtimeConfig) {
    return new ImportTraverser<>(key -> Lists.newLinkedList(getImportedBy(key, runtimeConfig)),
        this.recursiveImportedByMap).traverseGraphRecursively(configKey);
  }
}
