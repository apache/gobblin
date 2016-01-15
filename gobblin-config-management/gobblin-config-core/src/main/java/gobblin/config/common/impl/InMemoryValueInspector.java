package gobblin.config.common.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

import com.typesafe.config.Config;

import gobblin.config.store.api.ConfigKeyPath;

/**
 * InMemoryValueInspector provide the caching layer for getting the {@link com.typesafe.config.Config} from {@link ConfigStore}
 * 
 * @author mitu
 *
 */
public class InMemoryValueInspector implements ConfigStoreValueInspector{

  private final ConfigStoreValueInspector valueFallback;

  private final Map<ConfigKeyPath, Config> ownConfigMap;
  private final Map<ConfigKeyPath, Config> recursiveConfigMap;

  /**
   * 
   * @param valueFallback - the fall back {@link ConfigStoreValueInspector} which used to get the raw {@link com.typesafe.config.Config} 
   * @param useStrongRef  - if true, use Strong reference in cache, else, use Weak reference in cache
   */
  public InMemoryValueInspector (ConfigStoreValueInspector valueFallback, boolean useStrongRef){
    this.valueFallback = valueFallback;

    if(useStrongRef){
      this.ownConfigMap = new HashMap<>();
      this.recursiveConfigMap = new HashMap<>();
    }
    else{
      this.ownConfigMap = new WeakHashMap<>();
      this.recursiveConfigMap = new WeakHashMap<>();
    }
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If present in the cache, return the cached {@link com.typesafe.config.Config} for given input
   *   Otherwise, simply delegate the functionality to the internal {ConfigStoreValueInspector} and store the value into cache
   * </p>
   */
  @Override
  public Config getOwnConfig(ConfigKeyPath configKey) {
    if(this.ownConfigMap.containsKey(configKey)){
      Config result = this.ownConfigMap.get(configKey);
      // in case of the GC happened when containsKey() and get() function
      if(result!=null){
        return result;
      }
    }

    Config ownConfig = this.valueFallback.getOwnConfig(configKey);
    this.ownConfigMap.put(configKey, ownConfig);
    return ownConfig;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>
   *   If present in the cache, return the cached {@link com.typesafe.config.Config} for given input
   *   Otherwise, simply delegate the functionality to the internal {ConfigStoreValueInspector} and store the value into cache
   * </p>
   */
  @Override
  public Config getResolvedConfig(ConfigKeyPath configKey) {
    if(this.recursiveConfigMap.containsKey(configKey)){
      Config result = this.recursiveConfigMap.get(configKey);
      // in case of the GC happened when containsKey() and get() function
      if(result!=null){
        return result;
      }
    }

    Config recursiveConfig = this.valueFallback.getResolvedConfig(configKey);
    this.recursiveConfigMap.put(configKey, recursiveConfig);
    return recursiveConfig;
  }
}
