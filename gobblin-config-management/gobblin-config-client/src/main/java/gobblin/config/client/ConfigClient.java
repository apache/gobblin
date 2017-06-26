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

package gobblin.config.client;

import java.lang.annotation.Annotation;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;

import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.common.impl.ConfigStoreBackedTopology;
import gobblin.config.common.impl.ConfigStoreBackedValueInspector;
import gobblin.config.common.impl.ConfigStoreTopologyInspector;
import gobblin.config.common.impl.ConfigStoreValueInspector;
import gobblin.config.common.impl.InMemoryTopology;
import gobblin.config.common.impl.InMemoryValueInspector;
import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.ConfigStoreFactory;
import gobblin.config.store.api.ConfigStoreWithStableVersioning;
import gobblin.config.store.api.VersionDoesNotExistException;


/**
 * This class is used by Client to access the Configuration Management core library.
 *
 *
 * @author mitu
 *
 */
public class ConfigClient {
  private static final Logger LOG = Logger.getLogger(ConfigClient.class);

  private final VersionStabilityPolicy policy;

  /** Normally key is the ConfigStore.getStoreURI(), value is the ConfigStoreAccessor
   *
   * However, there may be two entries for a specific config store, for example
   * if user pass in URI like "etl-hdfs:///datasets/a1/a2" and the etl-hdfs config store factory using
   * default authority/default config store root normalized the URI to
   * "etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/datasets/a1/a2"
   * where /user/mitu/HdfsBasedConfigTest is the config store root
   *
   * Then there will be two entries in the Map which point to the same value
   * key1: "etl-hdfs:/"
   * key2: "etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/"
   *
   */
  private final TreeMap<URI, ConfigStoreAccessor> configStoreAccessorMap = new TreeMap<>();

  private final ConfigStoreFactoryRegister configStoreFactoryRegister;

  private ConfigClient(VersionStabilityPolicy policy) {
    this(policy, new ConfigStoreFactoryRegister());
  }

  @VisibleForTesting
  ConfigClient(VersionStabilityPolicy policy, ConfigStoreFactoryRegister register) {
    this.policy = policy;

    this.configStoreFactoryRegister = register;
  }

  /**
   * Create the {@link ConfigClient} based on the {@link VersionStabilityPolicy}.
   * @param policy - {@link VersionStabilityPolicy} to specify the stability policy which control the caching layer creation
   * @return       - {@link ConfigClient} for client to use to access the {@link ConfigStore}
   */
  public static ConfigClient createConfigClient(VersionStabilityPolicy policy) {
    return new ConfigClient(policy);
  }

  /**
   * Get the resolved {@link Config} based on the input URI.
   *
   * @param configKeyUri - The URI for the configuration key. There are two types of URI:
   *
   * 1. URI missing authority and configuration store root , for example "etl-hdfs:///datasets/a1/a2". It will get
   *    the configuration based on the default {@link ConfigStore} in etl-hdfs {@link ConfigStoreFactory}
   * 2. Complete URI:  for example "etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest/"
   *
   * @return  the resolved {@link Config} based on the input URI.
   *
   * @throws ConfigStoreFactoryDoesNotExistsException: if missing scheme name or the scheme name is invalid
   * @throws ConfigStoreCreationException: Specified {@link ConfigStoreFactory} can not create required {@link ConfigStore}
   * @throws VersionDoesNotExistException: Required version does not exist anymore ( may get deleted by retention job )
   */
  public Config getConfig(URI configKeyUri)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {
    return getConfig(configKeyUri, Optional.<Config>absent());
  }

  public Config getConfig(URI configKeyUri, Optional<Config> runtimeConfig)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {
    ConfigStoreAccessor accessor = this.getConfigStoreAccessor(configKeyUri);
    ConfigKeyPath configKeypath = ConfigClientUtils.buildConfigKeyPath(configKeyUri, accessor.configStore);
    return accessor.valueInspector.getResolvedConfig(configKeypath, runtimeConfig);
  }

  /**
   * batch process for {@link #getConfig(URI)} method
   * @param configKeyUris
   * @return
   * @throws ConfigStoreFactoryDoesNotExistsException
   * @throws ConfigStoreCreationException
   * @throws VersionDoesNotExistException
   */
  public Map<URI, Config> getConfigs(Collection<URI> configKeyUris)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {
    if (configKeyUris == null || configKeyUris.size() == 0)
      return Collections.emptyMap();

    Map<URI, Config> result = new HashMap<>();
    Multimap<ConfigStoreAccessor, ConfigKeyPath> partitionedAccessor = ArrayListMultimap.create();

    // map contains the mapping between ConfigKeyPath back to original URI , partitioned by ConfigStoreAccessor
    Map<ConfigStoreAccessor, Map<ConfigKeyPath, URI>> reverseMap = new HashMap<>();

    // partitioned the ConfigKeyPaths which belongs to the same store to one accessor
    for (URI u : configKeyUris) {
      ConfigStoreAccessor accessor = this.getConfigStoreAccessor(u);
      ConfigKeyPath configKeypath = ConfigClientUtils.buildConfigKeyPath(u, accessor.configStore);
      partitionedAccessor.put(accessor, configKeypath);

      if (!reverseMap.containsKey(accessor)) {
        reverseMap.put(accessor, new HashMap<ConfigKeyPath, URI>());
      }
      reverseMap.get(accessor).put(configKeypath, u);
    }

    for (Map.Entry<ConfigStoreAccessor, Collection<ConfigKeyPath>> entry : partitionedAccessor.asMap().entrySet()) {
      Map<ConfigKeyPath, Config> batchResult = entry.getKey().valueInspector.getResolvedConfigs(entry.getValue());

      for (Map.Entry<ConfigKeyPath, Config> resultEntry : batchResult.entrySet()) {
        // get the original URI from reverseMap
        URI orgURI = reverseMap.get(entry.getKey()).get(resultEntry.getKey());
        result.put(orgURI, resultEntry.getValue());
      }
    }

    return result;
  }

  /**
   * Convenient method to get resolved {@link Config} based on String input.
   */
  public Config getConfig(String configKeyStr) throws ConfigStoreFactoryDoesNotExistsException,
      ConfigStoreCreationException, VersionDoesNotExistException, URISyntaxException {
    return getConfig(configKeyStr, Optional.<Config>absent());
  }

  public Config getConfig(String configKeyStr, Optional<Config> runtimeConfig)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException,
             URISyntaxException {
    return this.getConfig(new URI(configKeyStr), runtimeConfig);
  }


  /**
   * batch process for {@link #getConfig(String)} method
   * @param configKeyStrs
   * @return
   * @throws ConfigStoreFactoryDoesNotExistsException
   * @throws ConfigStoreCreationException
   * @throws VersionDoesNotExistException
   * @throws URISyntaxException
   */
  public Map<URI, Config> getConfigsFromStrings(Collection<String> configKeyStrs)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException,
      URISyntaxException {
    if (configKeyStrs == null || configKeyStrs.size() == 0)
      return Collections.emptyMap();

    Collection<URI> configKeyUris = new ArrayList<>();
    for (String s : configKeyStrs) {
      configKeyUris.add(new URI(s));
    }
    return getConfigs(configKeyUris);
  }

  /**
   * Get the import links of the input URI.
   *
   * @param configKeyUri - The URI for the configuration key.
   * @param recursive    - Specify whether to get direct import links or recursively import links
   * @return  the import links of the input URI.
   *
   * @throws ConfigStoreFactoryDoesNotExistsException: if missing scheme name or the scheme name is invalid
   * @throws ConfigStoreCreationException: Specified {@link ConfigStoreFactory} can not create required {@link ConfigStore}
   * @throws VersionDoesNotExistException: Required version does not exist anymore ( may get deleted by retention job )
   */
  public Collection<URI> getImports(URI configKeyUri, boolean recursive)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {
    return getImports(configKeyUri, recursive, Optional.<Config>absent());
  }

  public Collection<URI> getImports(URI configKeyUri, boolean recursive, Optional<Config> runtimeConfig)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {
    ConfigStoreAccessor accessor = this.getConfigStoreAccessor(configKeyUri);
    ConfigKeyPath configKeypath = ConfigClientUtils.buildConfigKeyPath(configKeyUri, accessor.configStore);
    Collection<ConfigKeyPath> result;

    if (!recursive) {
      result = accessor.topologyInspector.getOwnImports(configKeypath, runtimeConfig);
    } else {
      result = accessor.topologyInspector.getImportsRecursively(configKeypath, runtimeConfig);
    }

    return ConfigClientUtils.buildUriInClientFormat(result, accessor.configStore, configKeyUri.getAuthority() != null);
  }

  /**
   * Get the URIs which imports the input URI
   *
   * @param configKeyUri - The URI for the configuration key.
   * @param recursive    - Specify whether to get direct or recursively imported by links
   * @return  the URIs which imports the input URI
   *
   * @throws ConfigStoreFactoryDoesNotExistsException: if missing scheme name or the scheme name is invalid
   * @throws ConfigStoreCreationException: Specified {@link ConfigStoreFactory} can not create required {@link ConfigStore}
   * @throws VersionDoesNotExistException: Required version does not exist anymore ( may get deleted by retention job )
   */
  public Collection<URI> getImportedBy(URI configKeyUri, boolean recursive)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {
    return getImportedBy(configKeyUri, recursive, Optional.<Config>absent());
  }

  public Collection<URI> getImportedBy(URI configKeyUri, boolean recursive, Optional<Config> runtimeConfig)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {
    ConfigStoreAccessor accessor = this.getConfigStoreAccessor(configKeyUri);
    ConfigKeyPath configKeypath = ConfigClientUtils.buildConfigKeyPath(configKeyUri, accessor.configStore);
    Collection<ConfigKeyPath> result;

    if (!recursive) {
      result = accessor.topologyInspector.getImportedBy(configKeypath, runtimeConfig);
    } else {
      result = accessor.topologyInspector.getImportedByRecursively(configKeypath, runtimeConfig);
    }

    return ConfigClientUtils.buildUriInClientFormat(result, accessor.configStore, configKeyUri.getAuthority() != null);
  }

  private URI getMatchedFloorKeyFromCache(URI configKeyURI) {
    URI floorKey = this.configStoreAccessorMap.floorKey(configKeyURI);
    if (floorKey == null) {
      return null;
    }

    // both scheme name and authority name, if present, should match
    // or both authority should be null
    if (ConfigClientUtils.isAncestorOrSame(configKeyURI, floorKey)) {
      return floorKey;
    }

    return null;
  }

  private ConfigStoreAccessor createNewConfigStoreAccessor(URI configKeyURI)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {

    LOG.info("Create new config store accessor for URI " + configKeyURI);
    ConfigStoreAccessor result;
    ConfigStoreFactory<ConfigStore> csFactory = this.getConfigStoreFactory(configKeyURI);
    ConfigStore cs = csFactory.createConfigStore(configKeyURI);

    if (!isConfigStoreWithStableVersion(cs)) {
      if (this.policy == VersionStabilityPolicy.CROSS_JVM_STABILITY) {
        throw new RuntimeException(String.format("with policy set to %s, can not connect to unstable config store %s",
            VersionStabilityPolicy.CROSS_JVM_STABILITY, cs.getStoreURI()));
      }
    }

    String currentVersion = cs.getCurrentVersion();
    LOG.info("Current config store version number: " + currentVersion);
    // topology related
    ConfigStoreBackedTopology csTopology = new ConfigStoreBackedTopology(cs, currentVersion);
    InMemoryTopology inMemoryTopology = new InMemoryTopology(csTopology);

    // value related
    ConfigStoreBackedValueInspector rawValueInspector =
        new ConfigStoreBackedValueInspector(cs, currentVersion, inMemoryTopology);
    InMemoryValueInspector inMemoryValueInspector;

    // ConfigStoreWithStableVersioning always create Soft reference cache
    if (isConfigStoreWithStableVersion(cs) || this.policy == VersionStabilityPolicy.WEAK_LOCAL_STABILITY) {
      inMemoryValueInspector = new InMemoryValueInspector(rawValueInspector, false);
      result = new ConfigStoreAccessor(cs, inMemoryValueInspector, inMemoryTopology);
    }
    // Non ConfigStoreWithStableVersioning but require STRONG_LOCAL_STABILITY, use Strong reference cache
    else if (this.policy == VersionStabilityPolicy.STRONG_LOCAL_STABILITY) {
      inMemoryValueInspector = new InMemoryValueInspector(rawValueInspector, true);
      result = new ConfigStoreAccessor(cs, inMemoryValueInspector, inMemoryTopology);
    }
    // Require No cache
    else {
      result = new ConfigStoreAccessor(cs, rawValueInspector, inMemoryTopology);
    }

    return result;
  }

  private static boolean isConfigStoreWithStableVersion(ConfigStore cs) {
    for (Annotation annotation : cs.getClass().getDeclaredAnnotations()) {
      if (annotation instanceof ConfigStoreWithStableVersioning) {
        return true;
      }
    }
    return false;
  }

  private ConfigStoreAccessor getConfigStoreAccessor(URI configKeyURI)
      throws ConfigStoreFactoryDoesNotExistsException, ConfigStoreCreationException, VersionDoesNotExistException {

    URI matchedFloorKey = getMatchedFloorKeyFromCache(configKeyURI);
    ConfigStoreAccessor result;
    if (matchedFloorKey != null) {
      result = this.configStoreAccessorMap.get(matchedFloorKey);
      return result;
    }

    result = createNewConfigStoreAccessor(configKeyURI);
    ConfigStore cs = result.configStore;

    // put default root URI in cache as well for the URI which missing authority
    if (configKeyURI.getAuthority() == null) {
      // configKeyURI is missing authority/configstore root "etl-hdfs:///datasets/a1/a2"
      try {
        this.configStoreAccessorMap.put(new URI(configKeyURI.getScheme(), null, "/", null, null), result);
      } catch (URISyntaxException e) {
        // should not come here
        throw new RuntimeException("Can not build URI based on " + configKeyURI);
      }
    } else {
      // need to check Config Store's root is the prefix of input configKeyURI
      if (!ConfigClientUtils.isAncestorOrSame(configKeyURI, cs.getStoreURI())) {
        throw new RuntimeException(
            String.format("Config Store root URI %s is not the prefix of input %s", cs.getStoreURI(), configKeyURI));
      }

    }

    // put to cache
    this.configStoreAccessorMap.put(cs.getStoreURI(), result);

    return result;
  }

  // use serviceLoader to load configStoreFactories
  @SuppressWarnings("unchecked")
  private ConfigStoreFactory<ConfigStore> getConfigStoreFactory(URI configKeyUri)
      throws ConfigStoreFactoryDoesNotExistsException {
    @SuppressWarnings("rawtypes")
    ConfigStoreFactory csf = this.configStoreFactoryRegister.getConfigStoreFactory(configKeyUri.getScheme());
    if (csf == null) {
      throw new ConfigStoreFactoryDoesNotExistsException(configKeyUri.getScheme(), "scheme name does not exists");
    }

    return csf;
  }

  static class ConfigStoreAccessor {
    final ConfigStore configStore;
    final ConfigStoreValueInspector valueInspector;
    final ConfigStoreTopologyInspector topologyInspector;

    ConfigStoreAccessor(ConfigStore cs, ConfigStoreValueInspector valueInspector,
        ConfigStoreTopologyInspector topologyInspector) {
      this.configStore = cs;
      this.valueInspector = valueInspector;
      this.topologyInspector = topologyInspector;
    }
  }
}
