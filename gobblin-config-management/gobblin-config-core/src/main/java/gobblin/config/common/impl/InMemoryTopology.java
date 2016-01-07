package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;

/**
 * InMemoryTopology will return stale data if the internal config store is Not {@ConfigStoreWithStableVersioning}
 * 
 * @author mitu
 *
 */
public class InMemoryTopology implements ConfigStoreTopologyIntf {

  private final ConfigStoreBackedTopology fallback;

  private final ListMultimap<ConfigKeyPath, ConfigKeyPath> childrenMap = ArrayListMultimap.create();
  private final ListMultimap<ConfigKeyPath, ConfigKeyPath> ownImportMap = ArrayListMultimap.create();
  private final HashMultimap<ConfigKeyPath, ConfigKeyPath> ownImportedByMap = HashMultimap.create();
  private final ListMultimap<ConfigKeyPath, ConfigKeyPath> recursiveImportMap = ArrayListMultimap.create();
  private final HashMultimap<ConfigKeyPath, ConfigKeyPath> recursiveImportedByMap = HashMultimap.create();

  public InMemoryTopology(ConfigStoreBackedTopology fallback) {
    this.fallback = fallback;
  }

  private void loadRawTopologyFromConfigStore() {
    // breath first search the whole topology
    // calls to retrieve cache / set cache if not present
    Collection<ConfigKeyPath> nodeQueue = this.getChildren(SingleLinkedListConfigKeyPath.ROOT);

    while(!nodeQueue.isEmpty()){
      ConfigKeyPath configKeyPath = nodeQueue.iterator().next();
      nodeQueue.remove(configKeyPath);

      // calls to retrieve cache / set cache if not present
      List<ConfigKeyPath> ownImports = this.getOwnImports(configKeyPath);
      for(ConfigKeyPath p: ownImports){
        this.ownImportedByMap.put(p, configKeyPath);
      }

      List<ConfigKeyPath> recursiveImports = null;
      // result may in the cache already 
      if(this.recursiveImportMap.containsKey(configKeyPath)){
        recursiveImports = this.recursiveImportMap.get(configKeyPath);
      }
      else {
        recursiveImports = SimpleImportChainResolver.getImportsRecursively(this.fallback.getConfigStore(), 
            this.fallback.getVersion(), configKeyPath);
        this.recursiveImportMap.putAll(configKeyPath, recursiveImports); 
      }

      if(recursiveImports!=null){
        for(ConfigKeyPath p: recursiveImports){
          this.recursiveImportedByMap.put(p, configKeyPath);
        }
      }

      // calls to retrieve cache / set cache if not present
      Collection<ConfigKeyPath> tmp = this.getChildren(configKeyPath);
      nodeQueue.addAll(tmp);
    }
  }

  @Override
  public Collection<ConfigKeyPath> getChildren(ConfigKeyPath configKey) {
    if (this.childrenMap.containsKey(configKey)) {
      return this.childrenMap.get(configKey);
    }

    Collection<ConfigKeyPath> result = this.fallback.getChildren(configKey);
    this.childrenMap.putAll(configKey, result);
    return result;
  }

  @Override
  public List<ConfigKeyPath> getOwnImports(ConfigKeyPath configKey) {
    if (this.ownImportMap.containsKey(configKey)) {
      return this.ownImportMap.get(configKey);
    }

    List<ConfigKeyPath> result = this.fallback.getOwnImports(configKey);
    this.ownImportMap.putAll(configKey, result);
    return result;
  }

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
      loadRawTopologyFromConfigStore();
      return this.ownImportedByMap.get(configKey);
    }
  }

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
      loadRawTopologyFromConfigStore();
      return this.recursiveImportMap.get(configKey);
    }
  }

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
      loadRawTopologyFromConfigStore();
      return this.recursiveImportedByMap.get(configKey);
    }
  }

}
