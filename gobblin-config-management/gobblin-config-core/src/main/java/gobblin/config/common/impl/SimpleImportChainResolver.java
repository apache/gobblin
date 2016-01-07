package gobblin.config.common.impl;

import gobblin.config.store.api.ConfigKeyPath;
import gobblin.config.store.api.ConfigStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SimpleImportChainResolver {

  /**
   * Obtain the recursively imported {@ConfigKeyPath} for the input config key per configuration store per version
   * 
   * @param cs          The ConfigStore to obtain recursively imported ConfigKeyPath list
   * @param version     The Version to obtain recursively imported ConfigKeyPath list
   * @param configKey   The ConfigKeyPath whose recursively imported ConfigKeyPath list is needed
   * @return            The recursively imported {@ConfigKeyPath} for the input config key
   */
  public static List<ConfigKeyPath> getImportsRecursively(ConfigStore cs, String version, ConfigKeyPath configKey){
    return getImportsRecursivelyHelper(cs, version, configKey, configKey, new ArrayList<ConfigKeyPath>());
  }
  
  private static List<ConfigKeyPath> getImportsRecursivelyHelper(ConfigStore cs, String version, ConfigKeyPath initialConfigKey, 
      ConfigKeyPath currentConfigKey, List<ConfigKeyPath> previous) {
    
    for (ConfigKeyPath p : previous) {
      if (currentConfigKey != null && currentConfigKey.equals(p)) {
        previous.add(p);
        throw new CircularDependencyException(getChain(initialConfigKey, previous, currentConfigKey));
      }
    }
    
    // root can not include anything, otherwise will have circular dependency
    if (currentConfigKey.isRootPath()) {
      return Collections.emptyList();
    }

    List<ConfigKeyPath> result = new ArrayList<>();

    // The order is already decided in imported List : higher priority appear in the head
    List<ConfigKeyPath> imported = cs.getOwnImports(currentConfigKey, version);
    
    // implicit import parent with the lowest priority
    imported.add(currentConfigKey.getParent());
    
    for (ConfigKeyPath u : imported) {
      result.add(u);
      
      List<ConfigKeyPath> current = new ArrayList<ConfigKeyPath>();
      current.addAll(previous);
      current.add(currentConfigKey);
      
      result.addAll(getImportsRecursivelyHelper(cs, version, initialConfigKey, u, current));
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

  private static String getChain(ConfigKeyPath initialConfigKey, List<ConfigKeyPath> chain, ConfigKeyPath circular) {
    StringBuilder sb = new StringBuilder();
    sb.append("Initial configKey : " + initialConfigKey);
    for (ConfigKeyPath u : chain) {
      sb.append(" -> " + u);
    }

    sb.append(" the configKey causing circular dependency: " + circular);
    return sb.toString();
  }
}
